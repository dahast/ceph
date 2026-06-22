// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include <exception>
#include <ranges>
#include <shared_mutex> // for std::shared_lock
#include <utility>
#include <vector>

#include <boost/asio/awaitable.hpp>
#include <boost/asio/bind_cancellation_slot.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/experimental/awaitable_operators.hpp>

#include <boost/container/flat_set.hpp>
#include <boost/container/flat_map.hpp>

#include <boost/system/system_error.hpp>
#include <boost/system/generic_category.hpp>

#include "include/fs_types.h"
#include "include/neorados/RADOS.hpp"

#include "common/async/blocked_completion.h"
#include "common/async/co_throttle.h"
#include "common/async/librados_completion.h"
#include "common/async/yield_context.h"

#include "common/dout.h"
#include "common/containers.h"
#include "common/error_code.h"

#include "neorados/cls/fifo.h"
#include "neorados/cls/log.h"
#include "neorados/cls/sem_set.h"

#include "rgw_asio_thread.h"
#include "rgw_bucket.h"
#include "rgw_bucket_layout.h"
#include "rgw_datalog.h"
#include "rgw_log_backing.h"
#include "rgw_tools.h"
#include "rgw_sal_rados.h"

static constexpr auto dout_subsys = ceph_subsys_rgw;

using namespace std::literals;

namespace ranges = std::ranges;

namespace sys = boost::system;

namespace nlog = ::neorados::cls::log;
namespace fifo = ::neorados::cls::fifo;
namespace ss = neorados::cls::sem_set;

namespace async = ceph::async;
namespace buffer = ceph::buffer;

using ceph::containers::tiny_vector;

void rgw_data_change::dump(ceph::Formatter *f) const
{
  std::string type;
  switch (entity_type) {
    case ENTITY_TYPE_BUCKET:
      type = "bucket";
      break;
    default:
      type = "unknown";
  }
  encode_json("entity_type", type, f);
  encode_json("key", key, f);
  utime_t ut(timestamp);
  encode_json("timestamp", ut, f);
  encode_json("gen", gen, f);
}

void rgw_data_change::decode_json(JSONObj *obj) {
  std::string s;
  JSONDecoder::decode_json("entity_type", s, obj);
  if (s == "bucket") {
    entity_type = ENTITY_TYPE_BUCKET;
  } else {
    entity_type = ENTITY_TYPE_UNKNOWN;
  }
  JSONDecoder::decode_json("key", key, obj);
  utime_t ut;
  JSONDecoder::decode_json("timestamp", ut, obj);
  timestamp = ut.to_real_time();
  JSONDecoder::decode_json("gen", gen, obj);
}

std::list<rgw_data_change> rgw_data_change::generate_test_instances() {
  std::list<rgw_data_change> l;
  l.emplace_back();
  l.emplace_back();
  l.back().entity_type = ENTITY_TYPE_BUCKET;
  l.back().key = "bucket_name";
  l.back().timestamp = ceph::real_clock::zero();
  l.back().gen = 0;
  return l;
}

void rgw_data_change_log_entry::dump(Formatter *f) const
{
  encode_json("log_id", log_id, f);
  utime_t ut(log_timestamp);
  encode_json("log_timestamp", ut, f);
  encode_json("entry", entry, f);
}

void rgw_data_change_log_entry::decode_json(JSONObj *obj) {
  JSONDecoder::decode_json("log_id", log_id, obj);
  utime_t ut;
  JSONDecoder::decode_json("log_timestamp", ut, obj);
  log_timestamp = ut.to_real_time();
  JSONDecoder::decode_json("entry", entry, obj);
}

void rgw_data_notify_entry::dump(Formatter *f) const
{
  encode_json("key", key, f);
  encode_json("gen", gen, f);
}

boost::intrusive_ptr<RGWDataChangesBE> DataLogBackends::head() {
  std::unique_lock l(m);
  auto i = end();
  --i;
  return i->second;
}

void rgw_data_notify_entry::decode_json(JSONObj *obj) {
  JSONDecoder::decode_json("key", key, obj);
  JSONDecoder::decode_json("gen", gen, obj);
}

class RGWDataChangesOmap final : public RGWDataChangesBE {
  using centries = std::vector<cls::log::entry>;
  std::map<std::string, std::vector<std::string>> oids;

public:
  RGWDataChangesOmap(neorados::RADOS r,
		     neorados::IOContext loc,
		     RGWDataChangesLog& datalog,
		     uint64_t gen_id,
		     bool per_zonegroup)
    : RGWDataChangesBE(r, std::move(loc), datalog, gen_id, per_zonegroup) {
    for (const auto& zg_id : datalog.get_zonegroup_ids(per_zonegroup)) {
      oids[zg_id].reserve(datalog.get_num_shards());
      for (int i = 0; i < datalog.get_num_shards(); ++i) {
        oids[zg_id].push_back(datalog.get_oid(gen_id, zg_id, i));
      }
    }
  }
  ~RGWDataChangesOmap() override = default;

  void prepare(ceph::real_time ut, const std::string& key,
	       buffer::list&& entry, entries& out) override {
    if (!std::holds_alternative<centries>(out)) {
      ceph_assert(std::visit([](const auto& v) { return std::empty(v); }, out));
      out = centries();
    }

    cls::log::entry e{ut, {}, key, std::move(entry)};
    std::get<centries>(out).push_back(std::move(e));
  }

  asio::awaitable<void>
  push(const DoutPrefixProvider *dpp, const std::string& zg_id,
       int index, entries&& items) override {
    co_await r.execute(
      oids.at(zg_id)[index], loc,
      neorados::WriteOp{}.exec(nlog::add(std::get<centries>(items))),
      asio::use_awaitable);
    co_return;
  }

  void
  push(const DoutPrefixProvider *dpp, const std::string& zg_id,
       int index, ceph::real_time now, const std::string& key,
       buffer::list&& bl, asio::yield_context y) override {
    r.execute(oids.at(zg_id)[index], loc,
	      neorados::WriteOp{}.exec(nlog::add(now, {}, key, std::move(bl))),
	      y);
    return;
  }

  asio::awaitable<std::tuple<std::span<rgw_data_change_log_entry>,
			     std::string>>
  list(const DoutPrefixProvider* dpp, const std::string& zg_id,
       int index, std::span<rgw_data_change_log_entry> entries,
       std::string marker) override {
    std::vector<cls::log::entry> entrystore{entries.size()};

    try {
      auto [lentries, lmark] =
	co_await nlog::list(r, oids.at(zg_id)[index], loc, {}, {},
			    marker, entrystore, asio::use_awaitable);

      entries = entries.first(lentries.size());
      std::ranges::transform(lentries, std::begin(entries),
			     [](const auto& e) {
			       rgw_data_change_log_entry entry;
			       entry.log_id = e.id;
			       entry.log_timestamp = e.timestamp;
			       auto liter = e.data.cbegin();
			       decode(entry.entry, liter);
			       return entry;
			     });
      co_return std::make_tuple(std::move(entries), lmark);
    } catch (const buffer::error& err) {
      ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__
			 << ": failed to decode data changes log entry: "
			 << err.what() << dendl;
      throw;
    } catch (const sys::system_error& e) {
      if (e.code() == sys::errc::no_such_file_or_directory ||
          e.code() == ceph::buffer::errc::end_of_buffer) {
        co_return std::make_tuple(entries.first(0), std::string{});
      } else {
        ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__
              << ": failed to list " << oids.at(zg_id)[index]
              << ": " << e.what() << dendl;
	      throw;
      }
    }
  }

  asio::awaitable<RGWDataChangesLogInfo>
  get_info(const DoutPrefixProvider *dpp, const std::string& zg_id,
	   int index) override {
    try {
      auto header = co_await nlog::info(r, oids.at(zg_id)[index], loc,
					asio::use_awaitable);
      co_return RGWDataChangesLogInfo{.marker = header.max_marker,
				      .last_update = header.max_time};
    } catch (const sys::system_error& e) {
      if (e.code() == sys::errc::no_such_file_or_directory || 
          e.code() == ceph::buffer::errc::end_of_buffer) {
	co_return RGWDataChangesLogInfo{};
      }
      ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__
			 << ": failed to get info from "
			 << oids.at(zg_id)[index]
			 << ": " << e.what() << dendl;
      throw;
    }
  }

  asio::awaitable<void>
  trim(const DoutPrefixProvider *dpp, const std::string& zg_id,
       int index, std::string_view marker) override {
    try {
      co_await nlog::trim(r, oids.at(zg_id)[index], loc, {},
			  std::string{marker}, asio::use_awaitable);
      co_return;
    } catch (const sys::system_error& e) {
      if (e.code() == sys::errc::no_such_file_or_directory || 
          e.code() == ceph::buffer::errc::end_of_buffer) {
	co_return;
      } else {
	ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__
			   << ": failed to get trim "
			   << oids.at(zg_id)[index]
			   << ": " << e.what() << dendl;
	throw;
      }
    }
  }

  std::string_view max_marker() const override {
    return "99999999";
  }

  asio::awaitable<bool> is_empty(const DoutPrefixProvider* dpp) override {
    std::vector<cls::log::entry> entrystore{1};
    for (auto& [zg_id, zg_oids] : oids) {
      for (auto& oid : zg_oids) {
	try {
	  auto [entries, marker] =
	    co_await nlog::list(r, oid, loc, {}, {}, {}, entrystore,
				asio::use_awaitable);
	  if (!entries.empty()) {
	    co_return false;
	  }
	} catch (const sys::system_error& e) {
	  if (e.code() == sys::errc::no_such_file_or_directory ||
	      e.code() == ceph::buffer::errc::end_of_buffer) {
	    continue;
	  }
	  throw;
	}
      }
    }
    co_return true;
  }
};

class RGWDataChangesFIFO final : public RGWDataChangesBE {
  using centries = std::deque<buffer::list>;
  std::map<std::string, std::vector<std::unique_ptr<LazyFIFO>>> fifos;

public:
  RGWDataChangesFIFO(neorados::RADOS r,
		     neorados::IOContext loc,
		     RGWDataChangesLog& datalog,
		     uint64_t gen_id,
		     bool per_zonegroup)
    : RGWDataChangesBE(r, std::move(loc), datalog, gen_id, per_zonegroup) {
    for (const auto& zg_id : datalog.get_zonegroup_ids(per_zonegroup)) {
      fifos[zg_id].reserve(datalog.get_num_shards());
      for (int i = 0; i < datalog.get_num_shards(); ++i) {
        fifos[zg_id].push_back(std::make_unique<LazyFIFO>(
          r, datalog.get_oid(gen_id, zg_id, i), loc));
      }
    }
  }
  ~RGWDataChangesFIFO() override = default;

  void prepare(ceph::real_time, const std::string&,
	       buffer::list&& entry, entries& out) override {
    if (!std::holds_alternative<centries>(out)) {
      ceph_assert(std::visit([](auto& v) { return std::empty(v); }, out));
      out = centries();
    }
    std::get<centries>(out).push_back(std::move(entry));
  }

  asio::awaitable<void>
  push(const DoutPrefixProvider* dpp, const std::string& zg_id,
       int index, entries&& items) override {
    co_return co_await fifos.at(zg_id)[index]->push(
      dpp, std::get<centries>(items));
  }

  void
  push(const DoutPrefixProvider* dpp, const std::string& zg_id,
       int index, ceph::real_time, const std::string&,
       buffer::list&& bl, asio::yield_context y) override {
    fifos.at(zg_id)[index]->push(dpp, std::move(bl), y);
  }

  asio::awaitable<std::tuple<std::span<rgw_data_change_log_entry>,
			     std::string>>
  list(const DoutPrefixProvider* dpp, const std::string& zg_id,
       int index, std::span<rgw_data_change_log_entry> entries,
       std::string marker) override {
    try {
      std::vector<fifo::entry> log_entries{entries.size()};
      auto [lentries, outmark] =
	co_await fifos.at(zg_id)[index]->list(dpp, marker,
	  log_entries);
      entries = entries.first(lentries.size());
      std::ranges::transform(lentries, entries.begin(),
			     [](const auto& e) {
			       rgw_data_change_log_entry entry ;
			       entry.log_id = e.marker;
			       entry.log_timestamp = e.mtime;
			       auto liter = e.data.cbegin();
			       decode(entry.entry, liter);
			       return entry;
			     });
      co_return  std::make_tuple(std::move(entries),
				 outmark ? std::move(*outmark) : std::string{});
    } catch (const buffer::error& err) {
      ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__
			 << ": failed to decode data changes log entry: "
			 << err.what() << dendl;
      throw;
    }
  }

  asio::awaitable<RGWDataChangesLogInfo>
  get_info(const DoutPrefixProvider *dpp, const std::string& zg_id,
	   int index) override {
    auto [marker, last_update] =
      co_await fifos.at(zg_id)[index]->last_entry_info(dpp);
    co_return RGWDataChangesLogInfo{ .marker = marker,
				     .last_update = last_update };
  }

  asio::awaitable<void>
  trim(const DoutPrefixProvider *dpp, const std::string& zg_id,
       int index, std::string_view marker) override {
    try {
      co_await fifos.at(zg_id)[index]->trim(dpp,
        std::string{marker}, false);
    } catch (const sys::system_error& e) {
      if (e.code() != sys::errc::no_message_available) {
	ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__
			   << ": trim failed: " << e.what() << dendl;
	throw;
      }
    }
  }

  std::string_view max_marker() const override {
    static const auto max_mark = fifo::FIFO::max_marker();
    return std::string_view(max_mark);
  }

  asio::awaitable<bool> is_empty(const DoutPrefixProvider *dpp) override {
    std::vector<fifo::entry> entrystore;
    for (auto& [zg_id, zg_fifos] : fifos) {
      for (auto& fifo : zg_fifos) {
	auto [lentries, outmark] = co_await fifo->list(dpp, {}, entrystore);
	if (!lentries.empty()) {
	  co_return false;
	}
      }
    }
    co_return true;
  }
};

RGWDataChangesLog::RGWDataChangesLog(rgw::sal::RadosStore* driver)
  : cct(driver->ctx()), rados(driver->get_neorados()),
    executor(driver->get_io_context().get_executor()),
    num_shards(cct->_conf->rgw_data_log_num_shards),
    prefix(get_prefix()),
    changes(cct->_conf->rgw_data_log_changes_size) {}

RGWDataChangesLog::RGWDataChangesLog(CephContext *cct, bool log_data,
                                     neorados::RADOS rados,
                                     std::optional<int> num_shards,
                                     std::optional<uint64_t> sem_max_keys)
  : cct(cct), rados(rados), log_data(log_data), executor(rados.get_executor()),
      num_shards(num_shards ? *num_shards :
		 cct->_conf->rgw_data_log_num_shards),
      prefix(get_prefix()), changes(cct->_conf->rgw_data_log_changes_size),
      sem_max_keys(sem_max_keys ? *sem_max_keys : ss::max_keys) {}

asio::awaitable<gen_oids>
DataLogBackends::get_gen_oids(const logback_generation& g) {
  gen_oids oids;
  if (!g.per_zonegroup) {
    const std::string trim_lock_oid = datalog.get_trim_lock_oid();
    for (const std::string& zg_id : datalog.get_zonegroup_ids(false)) {
      for (int i = 0; i < datalog.get_num_shards(); ++i) {
        auto oid = datalog.get_oid(g.gen_id, zg_id, i);
        if (oid == trim_lock_oid)
          oids[oid] = remove_action::clear;
        else
          oids[oid] = remove_action::remove;
      }
    }
    co_return oids;
  }
  // For per-zonegroup generations, enumerate the pool to discover all shard
  // OIDs, including those of zonegroups that have since been deleted and are
  // therefore absent from the current zonegroup_ids list.
  const auto list_prefix = datalog.get_oid_list_prefix(g.gen_id);
  auto cursor = neorados::Cursor::begin();
  do {
    auto [ls, next] = co_await rados.enumerate_objects(
      loc, cursor, neorados::Cursor::end(), 1000, {}, asio::use_awaitable);
    for (const auto& entry : ls) {
      if (entry.oid.starts_with(list_prefix)) {
        oids[entry.oid] = remove_action::remove;
      }
    }
    cursor = std::move(next);
  } while (cursor != neorados::Cursor::end());
  co_return oids;
}

void DataLogBackends::handle_init(entries_t e) {
  std::unique_lock l(m);
  for (const auto& [gen_id, gen] : e) {
    if (gen.pruned) {
      lderr(datalog.cct)
	<< __PRETTY_FUNCTION__ << ":" << __LINE__
	<< ": ERROR: given empty generation: gen_id=" << gen_id << dendl;
    }
    if (count(gen_id) != 0) {
      lderr(datalog.cct)
	<< __PRETTY_FUNCTION__ << ":" << __LINE__
	<< ": ERROR: generation already exists: gen_id=" << gen_id << dendl;
    }
    try {
      switch (gen.type) {
      case log_type::omap:
	emplace(gen_id,
		boost::intrusive_ptr<RGWDataChangesBE>(
		  new RGWDataChangesOmap(rados, loc, datalog, gen_id,
					 gen.per_zonegroup)));
	break;
      case log_type::fifo:
	emplace(gen_id,
		boost::intrusive_ptr<RGWDataChangesBE>(
		  new RGWDataChangesFIFO(rados, loc, datalog, gen_id,
					 gen.per_zonegroup)));
	break;
      default:
	lderr(datalog.cct)
	  << __PRETTY_FUNCTION__ << ":" << __LINE__
	  << ": IMPOSSIBLE: invalid log type: gen_id=" << gen_id
	  << ", type" << gen.type << dendl;
	throw sys::system_error{EFAULT, sys::generic_category()};
      }
    } catch (const sys::system_error& err) {
      lderr(datalog.cct)
	  << __PRETTY_FUNCTION__ << ":" << __LINE__
	  << ": error setting up backend: gen_id=" << gen_id
	  << ", err=" << err.what() << dendl;
      throw;
    }
  }
}

void DataLogBackends::handle_new_gens(entries_t e) {
  handle_init(std::move(e));
}

void DataLogBackends::handle_empty_to(uint64_t new_tail) {
  std::unique_lock l(m);
  auto i = cbegin();
  if (i->first < new_tail) {
    return;
  }
  if (new_tail >= (cend() - 1)->first) {
    lderr(datalog.cct)
      << __PRETTY_FUNCTION__ << ":" << __LINE__
      << ": ERROR: attempt to trim head: new_tail=" << new_tail << dendl;
    throw sys::system_error(EFAULT, sys::system_category());
  }
  erase(i, upper_bound(new_tail));
}


int RGWDataChangesLog::start(const DoutPrefixProvider *dpp,
			     const RGWZone* zone,
			     const RGWZoneParams& zoneparams,
			     std::string _own_zonegroup_id,
			     std::vector<std::string> _zonegroup_ids,
			     bool per_zonegroup,
			     bool background_tasks) noexcept
{
  log_data = zone->log_data;
  try {
    // Blocking in startup code, not ideal, but won't hurt anything.
    asio::co_spawn(executor,
		   start(dpp, zoneparams.log_pool,
			 std::move(_own_zonegroup_id),
			 std::move(_zonegroup_ids),
			 per_zonegroup,
			 background_tasks, background_tasks,
			 background_tasks),
		   async::use_blocked);
  } catch (const sys::system_error& e) {
    ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__
		       << ": Failed to start datalog: " << e.what()
		       << dendl;
    return ceph::from_error_code(e.code());
  } catch (const std::exception& e) {
    ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__
		       << ": Failed to start datalog: " << e.what()
		       << dendl;
    return ceph::from_exception(std::current_exception());
  }
  return 0;
}

asio::awaitable<void>
RGWDataChangesLog::start(const DoutPrefixProvider *dpp,
			 const rgw_pool& log_pool,
			 std::string _own_zonegroup_id,
			 std::vector<std::string> _zonegroup_ids,
			 bool per_zonegroup,
			 bool recovery,
			 bool watch,
			 bool renew)
{
  own_zonegroup_id = std::move(_own_zonegroup_id);
  zonegroup_ids = std::move(_zonegroup_ids);

  down_flag = false;
  ran_background = (recovery || watch || renew);

  try {
    loc = co_await rgw::init_iocontext(dpp, *rados, log_pool,
				       rgw::create, asio::use_awaitable);
  } catch (const std::exception& e) {
    ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__
		       << ": Failed to initialized ioctx: " << e.what()
		       << ", pool=" << log_pool << dendl;
    throw;
  }

  try {
    bes = co_await logback_generations::init<DataLogBackends>(
      dpp, *rados, metadata_log_oid(), loc, log_type::fifo, *this);
  } catch (const std::exception& e) {
    ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__
		       << ": Error initializing backends: " << e.what()
		       << dendl;
    throw;
  }

  try {
    const auto entries = bes->get_entries();
    const auto& head_gen = (entries.end() - 1)->second;
    if (head_gen.per_zonegroup != per_zonegroup) {
      co_await bes->new_backing(dpp, head_gen.type, per_zonegroup);
    }
  } catch (const std::exception& e) {
    ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__
		       << ": Error transitioning per_zonegroup: " << e.what()
		       << dendl;
    throw;
  }

  if (!log_data) {
    co_return;
  }

  if (renew) {
    renew_future = asio::co_spawn(
      renew_strand,
      renew_run(),
      asio::bind_cancellation_slot(renew_signal.slot(),
				   asio::bind_executor(renew_strand,
						       asio::use_future)));
  }
  if (watch) {
    // Establish watch here so we won't be 'started up' until we're watching.
    const auto oid = get_recover_lock_oid();
    auto established = co_await establish_watch(dpp, oid);
    if (!established) {
      throw sys::system_error{ENOTCONN, sys::generic_category(),
			      "Unable to establish recovery watch!"};
    }
    watch_future = asio::co_spawn(
      watch_strand,
      watch_loop(),
      asio::bind_cancellation_slot(watch_signal.slot(),
				   asio::bind_executor(watch_strand,
						       asio::use_future)));
  }
  if (recovery) {
    // Recovery can run concurrent with normal operation, so we don't
    // have to block startup while we do all that I/O.
    recovery_future = asio::co_spawn(
      recovery_strand,
      recover(dpp),
      asio::bind_cancellation_slot(recovery_signal.slot(),
				   asio::bind_executor(recovery_strand,
						       asio::use_future)));
  }
  co_return;
}

asio::awaitable<bool>
RGWDataChangesLog::establish_watch(const DoutPrefixProvider* dpp,
				   std::string_view oid) {
  const auto queue_depth = num_shards * 128;
  try {
    co_await rados->execute(oid, loc, neorados::WriteOp{}.create(false),
			    asio::use_awaitable);
    watchcookie = co_await rados->watch(oid, loc, asio::use_awaitable,
					std::nullopt, queue_depth);
  } catch (const std::exception& e) {
    ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__
		       << ": Unable to start watch! Error: "
		       << e.what() << dendl;
    watchcookie = 0;
  }

  if (watchcookie == 0) {
    // Dump our current working set.
    co_await renew_entries(dpp);
  }

  co_return watchcookie != 0;
}

struct recovery_check {
  int64_t shard = 0;
  std::vector<std::string> keys;

  recovery_check() = default;

  recovery_check(uint64_t shard, std::vector<std::string> keys)
    : shard(shard), keys(std::move(keys)) {}

  void encode(buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(shard, bl);
    encode(keys, bl);
    ENCODE_FINISH(bl);
  }

  void decode(buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(shard, bl);
    decode(keys, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(recovery_check);


struct recovery_reply {
  std::vector<unsigned> reply_set;

  recovery_reply() = default;

  recovery_reply(std::vector<unsigned> reply_set)
    : reply_set(std::move(reply_set)) {}

  void encode(buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(reply_set, bl);
    ENCODE_FINISH(bl);
  }

  void decode(buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(reply_set, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(recovery_reply);

asio::awaitable<void>
RGWDataChangesLog::process_notification(const DoutPrefixProvider* dpp,
					std::string_view oid) {
  auto notification = co_await rados->next_notification(watchcookie,
							asio::use_awaitable);
  recovery_check rc;
  // Don't send a reply if we get a bogus notification, we don't
  // want recovery to delete semaphores improperly.
  try {
    decode(rc, notification.bl);
  } catch (const std::exception& e) {
    ldpp_dout(dpp, 2) << "Got malformed notification: " << e.what() << dendl;
    co_return;
  }
  if (rc.shard >= num_shards) {
    ldpp_dout(dpp, 2) << "Got unknown shard " << rc.shard << dendl;
    co_return;
  }
  recovery_reply reply;
  reply.reply_set.resize(rc.keys.size(), 0);
  std::unique_lock l(lock);
  for (auto i = 0u; i < rc.keys.size(); ++i) {
    const auto& key = rc.keys[i];
    try {
      if (cur_cycle_contains(BucketGen{key})) {
	++reply.reply_set[i];
      }
    } catch (const std::exception&) {
      ldpp_dout(dpp, 2) << "Got invalid BucketGen key: " << key << dendl;
      co_return;
    }
    if (semaphores[rc.shard].contains(key)) {
      ++reply.reply_set[i];
    }
  }
  l.unlock();
  buffer::list replybl;
  encode(reply, replybl);
  try {
    co_await rados->notify_ack(oid, loc, notification.notify_id, watchcookie,
			       std::move(replybl), asio::use_awaitable);
  } catch (const std::exception& e) {
    ldpp_dout(dpp, 10) << __PRETTY_FUNCTION__
		       << ": Failed ack. Whatever server is in "
		       << "recovery won't decrement semaphores: "
		       << e.what() << dendl;
  }
}

asio::awaitable<void>
RGWDataChangesLog::watch_loop()
{
  const DoutPrefix dp(cct, dout_subsys, "rgw data changes log: ");
  const auto oid = get_recover_lock_oid();
  bool need_rewatch = false;

  while (!going_down()) {
    try {
      co_await process_notification(&dp, oid);
    } catch (const sys::system_error& e) {
      if (e.code() == neorados::errc::notification_overflow) {
	ldpp_dout(&dp, 10) << __PRETTY_FUNCTION__
			   << ": Notification overflow. Whatever server is in "
			   << "recovery won't decrement semaphores." << dendl;
	continue;
      }
      if (going_down() || e.code() == asio::error::operation_aborted){
	need_rewatch = false;
	break;
      } else {
	need_rewatch = true;
      }
    }
    if (need_rewatch) {
      try {
	if (watchcookie) {
	  auto wc = watchcookie;
	  watchcookie = 0;
	  co_await rados->unwatch(wc, loc, asio::use_awaitable);
	}
      } catch (const std::exception& e) {
	// Watch may not exist, don't care.
      }
      bool rewatched = false;
      ldpp_dout(&dp, 10) << __PRETTY_FUNCTION__
			 << ": Trying to re-establish watch" << dendl;

      rewatched = co_await establish_watch(&dp, oid);
      while (!rewatched) {
	boost::asio::steady_timer t(co_await asio::this_coro::executor, 500ms);
	co_await t.async_wait(asio::use_awaitable);
	ldpp_dout(&dp, 10) << __PRETTY_FUNCTION__
			   << ": Trying to re-establish watch" << dendl;
	rewatched = co_await establish_watch(&dp, oid);
      }
    }
  }
}

int RGWDataChangesLog::choose_shard_id(const rgw_bucket_shard& bs) {
  const auto& name = bs.bucket.name;
  auto shard_shift = (bs.shard_id > 0 ? bs.shard_id : 0);
  auto r = (ceph_str_hash_linux(name.data(), name.size()) +
	    shard_shift) % num_shards;
  return static_cast<int>(r);
}

asio::awaitable<void>
RGWDataChangesLog::renew_entries(const DoutPrefixProvider* dpp)
{
  if (!log_data) {
    co_return;
  }

  // batches: keyed by (shard_index, zg_id) so each bucket is pushed only to
  // the zonegroup it was registered for
  bc::flat_map<std::pair<int, std::string>, RGWDataChangesBE::entries> batches;

  std::unique_lock l(lock);
  decltype(cur_cycle) entries;
  entries.swap(cur_cycle);
  for (const auto& [bg, zg_id] : entries) {
    unsigned index = choose_shard_id(bg.shard);
    semaphores[index].insert(bg.get_key());
  }
  l.unlock();

  const auto now = real_clock::now();
  const auto expiration = now + ceph::make_timespan(cct->_conf->rgw_data_log_window);
  auto be = bes->head();

  for (const auto& [bg, zg_id] : entries) {
    auto index = choose_shard_id(bg.shard);
    rgw_data_change change;
    buffer::list bl;
    change.entity_type = ENTITY_TYPE_BUCKET;
    change.key = bg.shard.get_key();
    change.timestamp = now;
    change.gen = bg.gen;
    encode(change, bl);
    be->prepare(now, bg.shard.get_key(), std::move(bl), batches[{index, zg_id}]);
    update_renewed(bg.shard, bg.gen, expiration);
  }

  auto push_failed = false;
  for (auto& [key, batch] : batches) {
    const auto& [index, zg_id] = key;

    // Failure on push isn't fatal.
    try {
      co_await be->push(dpp, zg_id, index, std::move(batch));
    } catch (const std::exception& e) {
      push_failed = true;
      ldpp_dout(dpp, 5) << "RGWDataChangesLog::renew_entries(): Backend push failed "
			<< "with exception: " << e.what() << dendl;
    }
  }
  if (push_failed) {
    co_return;
  }

  // If we didn't error in pushing, we can now decrement the semaphores
  l.lock();
  for (auto index = 0u; index < unsigned(num_shards); ++index) {
    using neorados::WriteOp;
    auto& keys = semaphores[index];
    while (!keys.empty()) {
      bc::flat_set<std::string> batch;
      // Can't use a move iterator here, since the keys have to stay
      // until they're safely on the OSD to avoid the risk of
      // double-decrement from recovery.
      auto to_copy = std::min(sem_max_keys, keys.size());
      std::copy_n(keys.begin(), to_copy,
		  std::inserter(batch, batch.end()));
      auto op = WriteOp{}.exec(ss::decrement(std::move(batch)));
      l.unlock();
      co_await rados->execute(get_sem_set_oid(index), loc, std::move(op),
			      asio::use_awaitable);
      l.lock();
      auto iter = keys.cbegin();
      std::advance(iter, to_copy);
      keys.erase(keys.cbegin(), iter);
    }
  }
  co_return;
}

auto RGWDataChangesLog::_get_change(const rgw_bucket_shard& bs,
				    uint64_t gen)
  -> ChangeStatusPtr
{
  ChangeStatusPtr status;
  if (!changes.find({bs, gen}, status)) {
    status = std::make_shared<ChangeStatus>(rados->get_executor());
    changes.add({bs, gen}, status);
  }
  return status;
}

bool RGWDataChangesLog::cur_cycle_contains(const BucketGen& bg) const
{
  auto it = cur_cycle.lower_bound({bg, ""});
  return it != cur_cycle.end() && it->first == bg;
}

bool RGWDataChangesLog::register_renew(const std::string& zg_id, BucketGen bg)
{
  std::scoped_lock l{lock};
  bool already_present = cur_cycle_contains(bg);
  cur_cycle.emplace(bg, zg_id);
  return !already_present;
}

void RGWDataChangesLog::update_renewed(const rgw_bucket_shard& bs,
				       uint64_t gen,
				       real_time expiration)
{
  std::unique_lock l{lock};
  auto status = _get_change(bs, gen);
  l.unlock();

  ldout(cct, 20) << "RGWDataChangesLog::update_renewed() bucket_name="
		 << bs.bucket.name << " shard_id=" << bs.shard_id
		 << " expiration=" << expiration << dendl;

  std::unique_lock sl(status->lock);
  status->cur_expiration = expiration;
}

int RGWDataChangesLog::get_log_shard_id(rgw_bucket& bucket, int shard_id) {
  rgw_bucket_shard bs(bucket, shard_id);
  return choose_shard_id(bs);
}

bool RGWDataChangesLog::filter_bucket(const DoutPrefixProvider *dpp,
				      const rgw_bucket& bucket,
				      const std::string& /*zg_id*/,
				      asio::yield_context y) const
{
  if (!bucket_filter) {
    return true;
  }
  return bucket_filter(bucket, y, dpp);
}

std::string RGWDataChangesLog::get_oid(uint64_t gen_id,
				       std::string_view zg_id,
				       int shard_id) const {
  return (gen_id > 0 ?
    (zg_id.empty() ?
       fmt::format("{}@G{}.{}", prefix, gen_id, shard_id) :
       fmt::format("{}@G{}@Z{}.{}", prefix, gen_id, zg_id, shard_id)) :
    fmt::format("{}.{}", prefix, shard_id));
}

std::string RGWDataChangesLog::get_oid_list_prefix(uint64_t gen_id) {
  // only for gens with per_zonegroup == true (implies gen_id > 0)
  return fmt::format("{}@G{}@Z", prefix, gen_id);
}

std::string RGWDataChangesLog::get_sem_set_oid(int i) const {
  return fmt::format("_sem_set{}.{}", prefix, i);
}

std::string RGWDataChangesLog::metadata_log_oid() const {
  return fmt::format("{}generations_metadata", prefix);
}

std::string RGWDataChangesLog::get_trim_lock_oid() const {
  // historically use first data log shard of gen 0, keep the object name
  // regardless of introduction of generations or per_zonegroup data logs
  return get_oid(0, "", 0);
}

std::string RGWDataChangesLog::get_recover_lock_oid() const {
  // use sem_set shard 0
  return get_sem_set_oid(0);
}

asio::awaitable<void>
RGWDataChangesLog::add_entry(const DoutPrefixProvider* dpp,
			     const RGWBucketInfo& bucket_info,
			     const rgw::bucket_log_layout_generation& gen,
			     int shard_id)
{
  co_await asio::spawn(
    co_await asio::this_coro::executor,
    [this, dpp, &bucket_info, &gen, shard_id](asio::yield_context y) {
      return add_entry(dpp, bucket_info, gen, shard_id, y);
    }, asio::use_awaitable);
  co_return;
}


void RGWDataChangesLog::add_entry(const DoutPrefixProvider* dpp,
				  const RGWBucketInfo& bucket_info,
				  const rgw::bucket_log_layout_generation& gen,
				  int shard_id, asio::yield_context y)
{
  if (!log_data || down_flag) {
    return;
  }

  auto& bucket = bucket_info.bucket;
  rgw_bucket_shard bs(bucket, shard_id);
  int index = choose_shard_id(bs);
  auto be = bes->head();
  const auto& zg_ids = get_zonegroup_ids(be->per_zonegroup);

  // Determine which zonegroups this entry applies to.  filter_bucket may yield,
  // so collect results before acquiring any lock.
  std::vector<const std::string*> active_zg;
  for (const std::string& zg_id : zg_ids) {
    if (filter_bucket(dpp, bucket, zg_id, y)) {
      active_zg.push_back(&zg_id);
    }
  }
  if (active_zg.empty()) {
    return;
  }

  // Notify observer for own zonegroup (at most once).
  if (observer) {
    for (const std::string* zg : active_zg) {
      if (zg->empty() || *zg == own_zonegroup_id) {
        observer->on_bucket_changed(bucket.get_key());
        break;
      }
    }
  }

  if (!(watchcookie && rados->check_watch(watchcookie))) {
    // Bypass the window optimization: push directly to all active zonegroups.
    auto now = real_clock::now();
    ldpp_dout(dpp, 2) << "RGWDataChangesLog::add_entry(): "
		      << "Bypassing window optimization and pushing directly: "
		      << "bucket.name=" << bucket.name
		      << " shard_id=" << bs.shard_id << " now="
		      << now << " cur_expiration=" << dendl;

    buffer::list bl;
    rgw_data_change change;
    change.entity_type = ENTITY_TYPE_BUCKET;
    change.key = bs.get_key();
    change.timestamp = now;
    change.gen = gen.gen;
    encode(change, bl);

    // Failure on push is fatal if we're bypassing semaphores.
    for (auto it = active_zg.begin(); it != active_zg.end(); ++it) {
      buffer::list bl_to_push =
          (std::next(it) != active_zg.end()) ? bl : std::move(bl);
      be->push(dpp, **it, index, now, change.key, std::move(bl_to_push), y);
    }
    return;
  }

  for (const std::string* zg : active_zg) {
    mark_modified(*zg, index, bs, gen.gen);
  }

  // Window check: one ChangeStatus per BucketGen covers all zonegroups together.
  std::unique_lock l(lock);
  auto status = _get_change(bs, gen.gen);
  l.unlock();

  auto now = real_clock::now();
  std::unique_lock sl(status->lock);

  ldpp_dout(dpp, 20) << "RGWDataChangesLog::add_entry() bucket.name=" << bucket.name
		     << " shard_id=" << bs.shard_id << " now=" << now
		     << " cur_expiration=" << status->cur_expiration << dendl;

  if (now < status->cur_expiration) {
    /* no need to send, recently completed — queue all zonegroups for renewal */
    sl.unlock();
    auto bg = BucketGen{bs, gen.gen};
    const auto key = bg.get_key();
    for (const std::string* zg : active_zg) {
      auto need_sem_set = register_renew(*zg, bg);
      if (need_sem_set) {
        using neorados::WriteOp;
        rados->execute(get_sem_set_oid(index), loc,
          WriteOp{}.exec(ss::increment(key)), y);
      }
    }
    return;
  }

  if (status->pending) {
    // Another coroutine is pushing for this BucketGen; it covers all zonegroups.
    status->cond.async_wait(sl, y);
    sl.unlock();
    return;
  }

  status->cond.notify(sl);
  status->pending = true;
  status->cur_sent = now;
  sl.unlock();

  buffer::list bl;
  rgw_data_change change;
  change.entity_type = ENTITY_TYPE_BUCKET;
  change.key = bs.get_key();
  change.timestamp = now;
  change.gen = gen.gen;
  encode(change, bl);

  ldpp_dout(dpp, 20) << "RGWDataChangesLog::add_entry() sending update with now=" << now
		     << " cur_expiration="
		     << now + ceph::make_timespan(cct->_conf->rgw_data_log_window) << dendl;

  // Failure on push isn't fatal.
  for (auto it = active_zg.begin(); it != active_zg.end(); ++it) {
    try {
      buffer::list bl_to_push =
          (std::next(it) != active_zg.end()) ? bl : std::move(bl);
      be->push(dpp, **it, index, now, change.key, std::move(bl_to_push), y);
    } catch (const std::exception& e) {
      ldpp_dout(dpp, 5) << "RGWDataChangesLog::add_entry(): Backend push failed "
			<< "with exception: " << e.what() << dendl;
    }
  }

  now = real_clock::now();
  sl.lock();
  status->pending = false;
  /* time of when operation started, not completed */
  status->cur_expiration = status->cur_sent + make_timespan(cct->_conf->rgw_data_log_window);
  status->cond.notify(sl);
  sl.unlock();
}

int RGWDataChangesLog::add_entry(const DoutPrefixProvider* dpp,
				 const RGWBucketInfo& bucket_info,
				 const rgw::bucket_log_layout_generation& gen,
				 int shard_id, optional_yield y) noexcept
{
  try {
    if (y) {
      add_entry(dpp, bucket_info, gen, shard_id, y.get_yield_context());
    } else {
      maybe_warn_about_blocking(dpp);
      asio::spawn(rados->get_executor(),
		  [this, dpp, &bucket_info, &gen,
		   &shard_id](asio::yield_context y) {
		    add_entry(dpp, bucket_info, gen, shard_id, y);
		  }, async::use_blocked);
    }
  } catch (const std::exception&) {
    return ceph::from_exception(std::current_exception());
  }
  return 0;
}

const std::string&
RGWDataChangesLog::effective_zg_id(const RGWDataChangesBE& be,
                                    const std::string& zg_id) const
{
  if (!be.per_zonegroup)
    return no_zonegroup_ids[0];
  if (zg_id.empty())
    return own_zonegroup_id;
  return zg_id;
}

asio::awaitable<std::tuple<std::span<rgw_data_change_log_entry>,
			   std::string>>
DataLogBackends::list(const DoutPrefixProvider *dpp,
		      const std::string& zg_id, int shard_id,
		      std::span<rgw_data_change_log_entry> entries,
		      std::string marker)
{
  assert(shard_id < datalog.get_num_shards());
  const auto [start_id, // Starting generation
	      start_cursor // Cursor to be used when listing the
			   // starting generation
    ] = cursorgen(marker);
  auto gen_id = start_id; // Current generation being listed
  // Cursor with prepended generation, returned to caller
  std::string out_cursor;
  // Span to return to caller
  auto entries_out = entries;
  // Iterator (for inserting stuff into span)
  auto out = entries_out.begin();
  // Allocated storage for raw listings from backend
  std::vector<rgw_data_change_log_entry> gentries{entries.size()};
  while (out < entries_out.end()) {
    std::unique_lock l(m);
    auto i = lower_bound(gen_id);
    if (i == end()) {
      // done, no more gens
      out_cursor.clear();
      break;
    }
    auto be = i->second;
    l.unlock();
    gen_id = be->gen_id;
    auto inspan = std::span{gentries}.first(entries_out.end() - out);
    // Since later generations continue listings from the
    // first, start them at the beginning.
    auto incursor = gen_id == start_id ? start_cursor : std::string{};
    auto [raw_entries, raw_cursor]
      = co_await be->list(dpp, datalog.effective_zg_id(*be, zg_id),
                          shard_id, inspan, incursor);
    out = std::transform(std::make_move_iterator(raw_entries.begin()),
			 std::make_move_iterator(raw_entries.end()),
			 out, [gen_id](rgw_data_change_log_entry e) {
			   e.log_id = gencursor(gen_id, e.log_id);
			   return e;
			 });
    if (!raw_cursor.empty()) {
      // there are entries left in this gen, we are here because entries_out
      // is full, set new cursor and leave loop
      out_cursor = gencursor(gen_id, raw_cursor);
      break;
    }
    // gen exhauste, continue with next gen
    ++gen_id;
    // in case entries_out is full, have a correct our_coursor prepared
    out_cursor = gencursor(gen_id, std::string{});
  }
  entries_out = entries_out.first(out - entries_out.begin());
  co_return std::make_tuple(entries_out, std::move(out_cursor));
}

asio::awaitable<std::tuple<std::vector<rgw_data_change_log_entry>,
			   std::string, bool>>
RGWDataChangesLog::list_entries(const DoutPrefixProvider* dpp,
				const std::string& zg_id, int shard_id,
				int max_entries, std::string marker)
{
  if (shard_id >= num_shards) [[unlikely]] {
    throw sys::system_error{
      EINVAL, sys::generic_category(),
      fmt::format("{} is not a valid shard. Valid shards are integers in [0, {})",
		  shard_id, num_shards)};
  }
  if (max_entries <= 0) {
    co_return std::make_tuple(std::vector<rgw_data_change_log_entry>{},
			      std::string{}, false);
  }
  std::vector<rgw_data_change_log_entry> entries(max_entries);
  entries.resize(max_entries);
  auto [spanentries, outmark] = co_await bes->list(dpp, zg_id, shard_id, entries,
    marker);
  entries.resize(spanentries.size());
  bool truncated = !outmark.empty();
  co_return std::make_tuple(std::move(entries), std::move(outmark), truncated);
}

asio::awaitable<std::tuple<std::vector<rgw_data_change_log_entry>,
			   RGWDataChangesLogMarker, bool>>
RGWDataChangesLog::list_entries(const DoutPrefixProvider *dpp,
				const std::string& zg_id,
				int max_entries, RGWDataChangesLogMarker marker)
{
  if (max_entries <= 0) {
    co_return std::make_tuple(std::vector<rgw_data_change_log_entry>{},
			      RGWDataChangesLogMarker{}, false);
  }

  std::vector<rgw_data_change_log_entry> entries(max_entries);
  std::span remaining{entries};

  do {
    std::span<rgw_data_change_log_entry> outspan;
    std::string outmark;
    std::tie(outspan, outmark) = co_await bes->list(dpp, zg_id, marker.shard,
						    remaining, marker.marker);
    remaining = remaining.last(remaining.size() - outspan.size());
    if (!outmark.empty()) {
      marker.marker = std::move(outmark);
    } else if (outmark.empty() && marker.shard < (num_shards - 1)) {
      ++marker.shard;
      marker.marker.clear();
    } else {
      marker.clear();
    }
  } while (!remaining.empty() && marker);
  if (!remaining.empty()) {
    entries.resize(entries.size() - remaining.size());
  }
  bool truncated = marker;
  co_return std::make_tuple(std::move(entries), std::move(marker), truncated);
}

asio::awaitable<RGWDataChangesLogInfo>
RGWDataChangesLog::get_info(const DoutPrefixProvider* dpp,
			    const std::string& zg_id, int shard_id)
{
  if (shard_id >= num_shards) [[unlikely]] {
    throw sys::system_error{-EINVAL, sys::generic_category(),
      fmt::format(
	"{} is not a valid shard. Valid shards are integers in [0, {})",
	shard_id, num_shards)};
  }
  auto be = bes->head();
  auto info = co_await be->get_info(dpp, effective_zg_id(*be, zg_id), shard_id);
  if (!info.marker.empty()) {
    info.marker = gencursor(be->gen_id, info.marker);
  }
  co_return info;
}

asio::awaitable<void> DataLogBackends::trim_entries(
  const DoutPrefixProvider *dpp, int shard_id,
  std::string_view marker)
{
  assert(shard_id < datalog.get_num_shards());
  auto [target_gen, cursor] = cursorgen(std::string{marker});
  std::unique_lock l(m);

  const auto head_gen = (end() - 1)->second->gen_id;
  const auto tail_gen = begin()->first;
  if (target_gen < tail_gen)
    co_return;

  for (auto be = lower_bound(0)->second;
       be->gen_id <= target_gen && be->gen_id <= head_gen;
       be = upper_bound(be->gen_id)->second) {
    l.unlock();
    auto c = be->gen_id == target_gen ? cursor : be->max_marker();
    auto zg_ids = datalog.get_zonegroup_ids(be->per_zonegroup);
    for (const std::string& zg_id : zg_ids) {
      co_await be->trim(dpp, zg_id, shard_id, c);
    }
    if (be->gen_id == target_gen || be->gen_id >= head_gen)
      break;
    l.lock();
  };

  co_return;
}

asio::awaitable<void>
RGWDataChangesLog::trim_entries(const DoutPrefixProvider *dpp, int shard_id,
				std::string_view marker)
{
  if (shard_id >= num_shards) [[unlikely]] {
    throw sys::system_error{-EINVAL, sys::generic_category(),
      fmt::format(
	"{} is not a valid shard. Valid shards are integers in [0, {})",
	shard_id, num_shards)};
  }
  co_return co_await bes->trim_entries(dpp, shard_id, marker);
}

void RGWDataChangesLog::trim_entries(const DoutPrefixProvider* dpp,
				     int shard_id, std::string_view marker,
				     librados::AioCompletion* c)
{
  asio::co_spawn(rados->get_executor(),
		 trim_entries(dpp, shard_id, marker),
		 c);
}


asio::awaitable<void> DataLogBackends::trim_generations(
  const DoutPrefixProvider *dpp,
  std::optional<uint64_t>& through)
{
  if (size() != 1) {
    std::vector<mapped_type> candidates;
    {
      std::scoped_lock l(m);
      auto e = cend() - 1;
      for (auto i = cbegin(); i < e; ++i) {
	candidates.push_back(i->second);
      }
    }

    std::optional<uint64_t> highest;
    for (auto& be : candidates) {
      if (co_await be->is_empty(dpp)) {
	highest = be->gen_id;
      } else {
	break;
      }
    }

    through = highest;
    if (!highest) {
      co_return;
    }
    co_await empty_to(dpp, *highest);
  }

  co_await remove_empty(dpp);
  co_return;
}

bool RGWDataChangesLog::going_down() const
{
  return down_flag;
}

// Now, if we had an awaitable future…
asio::awaitable<void> RGWDataChangesLog::async_shutdown()
{
  DoutPrefix dp{cct, ceph_subsys_rgw, "Datalog Shutdown"};
  if (down_flag) {
    co_return;
  }
  down_flag = true;
  if (!ran_background)  {
    co_return;
  }
  renew_stop();
  // Revisit this later
  asio::dispatch(renew_strand,
		 [this]() {
		   renew_signal.emit(asio::cancellation_type::terminal);
		 });
  asio::dispatch(recovery_strand,
		 [this]() {
		   recovery_signal.emit(asio::cancellation_type::terminal);
		 });
  asio::dispatch(watch_strand,
		 [this]() {
		   watch_signal.emit(asio::cancellation_type::terminal);
		 });
  if (watchcookie && rados->check_watch(watchcookie)) {
    auto wc = watchcookie;
    watchcookie = 0;
    try {
      co_await rados->unwatch(wc, loc, asio::use_awaitable);
    } catch (const std::exception& e) {
      ldpp_dout(&dp, 2)
	<< "RGWDataChangesLog::async_shutdown: unwatch failed: " << e.what()
	<< dendl;
    }
  }
  co_return;
}

void RGWDataChangesLog::blocking_shutdown()
{
  DoutPrefix dp{cct, ceph_subsys_rgw, "Datalog Shutdown"};
  if (down_flag) {
    return;
  }
  down_flag = true;
  if (ran_background)  {
    renew_stop();
    // Revisit this later
    asio::dispatch(renew_strand,
		   [this]() {
		     renew_signal.emit(asio::cancellation_type::terminal);
		   });
    try {
      renew_future.wait();
    } catch (const std::future_error& e) {
      if (e.code() != std::future_errc::no_state) {
	throw;
      }
    }
    asio::dispatch(recovery_strand,
		   [this]() {
		     recovery_signal.emit(asio::cancellation_type::terminal);
		   });
    try {
      recovery_future.wait();
    } catch (const std::future_error& e) {
      if (e.code() != std::future_errc::no_state) {
	throw;
      }
    }
    asio::dispatch(watch_strand,
		   [this]() {
		     watch_signal.emit(asio::cancellation_type::terminal);
		   });
    try {
      watch_future.wait();
    } catch (const std::future_error& e) {
      if (e.code() != std::future_errc::no_state) {
	throw;
      }
    }
    if (watchcookie && rados->check_watch(watchcookie)) {
      auto wc = watchcookie;
      watchcookie = 0;
      try {
	rados->unwatch(wc, loc, async::use_blocked);
      } catch (const std::exception& e) {
	ldpp_dout(&dp, 2)
	  << "RGWDataChangesLog::blocking_shutdown: unwatch failed: " << e.what()
	  << dendl;
      }
    }
  }
  if (bes) {
    bes->shutdown();
    bes.reset();
  }
  return;
}

RGWDataChangesLog::~RGWDataChangesLog() {
  if (log_data && !down_flag) {
    lderr(cct) << __PRETTY_FUNCTION__ << ":" << __LINE__
	       << ": RGWDataChangesLog destructed without shutdown." << dendl;
  }
}

asio::awaitable<void> RGWDataChangesLog::renew_run() {
  static constexpr auto runs_per_prune = 150;
  auto run = 0;
  renew_timer.emplace(co_await asio::this_coro::executor);
  std::string_view operation;
  const DoutPrefix dp(cct, dout_subsys, "rgw data changes log: ");
  for (;;) try {
      ldpp_dout(&dp, 2) << "RGWDataChangesLog::ChangesRenewThread: start"
			<< dendl;
      operation = "RGWDataChangesLog::renew_entries"sv;
      co_await renew_entries(&dp);
      operation = {};
      if (going_down())
	break;

      if (run == runs_per_prune) {
	std::optional<uint64_t> through;
	ldpp_dout(&dp, 2) << "RGWDataChangesLog::ChangesRenewThread: pruning old generations" << dendl;
	operation = "trim_generations"sv;
	co_await trim_generations(&dp, through);
	operation = {};
	if (through) {
	  ldpp_dout(&dp, 2)
	    << "RGWDataChangesLog::ChangesRenewThread: pruned generations "
	    << "through " << *through << "." << dendl;
	} else {
	  ldpp_dout(&dp, 2)
	    << "RGWDataChangesLog::ChangesRenewThread: nothing to prune."
	    << dendl;
	}
        run = 0;
      } else {
	++run;
      }

      int interval = cct->_conf->rgw_data_log_window * 3 / 4;
      renew_timer->expires_after(std::chrono::seconds(interval));
      co_await renew_timer->async_wait(asio::use_awaitable);
    } catch (sys::system_error& e) {
      if (e.code() == asio::error::operation_aborted) {
	ldpp_dout(&dp, 10)
	  << "RGWDataChangesLog::renew_entries canceled, going down" << dendl;
	break;
      } else {
	ldpp_dout(&dp, 0)
	  << "renew_thread: ERROR: "
	  << (operation.empty() ? operation : "<unknown"sv)
	  << "threw exception: " << e.what() << dendl;
	continue;
      }
    }
}

void RGWDataChangesLog::renew_stop()
{
  std::lock_guard l{lock};
  if (renew_timer) {
    renew_timer->cancel();
  }
}

void RGWDataChangesLog::mark_modified(const std::string& zg_id, int shard_id,
				      const rgw_bucket_shard& bs, uint64_t gen)
{
  assert(shard_id < num_shards);
  if (!cct->_conf->rgw_data_notify_interval_msec) {
    return;
  }

  auto key = bs.get_key();
  {
    std::shared_lock rl{modified_lock}; // read lock to check for existence
    auto shard = modified_shards.find(shard_id);
    if (shard != modified_shards.end() && shard->second.count({key, gen})) {
      return;
    }
  }

  std::unique_lock wl{modified_lock}; // write lock for insertion
  modified_shards[shard_id].insert(rgw_data_notify_entry{key, gen});
}

std::string RGWDataChangesLog::max_marker() const {
  return gencursor(std::numeric_limits<uint64_t>::max(),
		   "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
}

asio::awaitable<void>
RGWDataChangesLog::change_format(const DoutPrefixProvider *dpp, log_type type)
{
  co_return co_await bes->new_backing(dpp, type, bes->head()->per_zonegroup);
}

asio::awaitable<void>
RGWDataChangesLog::trim_generations(const DoutPrefixProvider *dpp,
				    std::optional<uint64_t>& through)
{
  co_return co_await bes->trim_generations(dpp, through);
}

asio::awaitable<std::pair<bc::flat_map<std::string, uint64_t>,
			  std::string>>
RGWDataChangesLog::read_sems(int index, std::string cursor) {
  bc::flat_map<std::string, uint64_t> out;
  try {
    co_await rados->execute(
      get_sem_set_oid(index), loc,
      neorados::ReadOp{}.exec(ss::list(sem_max_keys, std::move(cursor),
				       &out, &cursor)),
      nullptr, asio::use_awaitable);
  } catch (const sys::system_error& e) {
    if (e.code() != sys::errc::no_such_file_or_directory &&
        e.code() != ceph::buffer::errc::end_of_buffer) {
      throw;
    }
  }
  co_return std::make_pair(std::move(out), std::move(cursor));
}

asio::awaitable<bool>
RGWDataChangesLog::synthesize_entries(
  const DoutPrefixProvider* dpp,
  int index,
  const bc::flat_map<std::string, uint64_t>& semcount)
{
  const auto timestamp = real_clock::now();
  auto be = bes->head();
  auto push_failed = false;

  RGWDataChangesBE::entries batch;
  for (const auto& [key, sem] : semcount) {
    try {
      BucketGen bg{key};
      rgw_data_change change;
      buffer::list bl;
      change.entity_type = ENTITY_TYPE_BUCKET;
      change.key = bg.shard.get_key();
      change.timestamp = timestamp;
      change.gen = bg.gen;
      encode(change, bl);
      be->prepare(timestamp, change.key, std::move(bl), batch);
    } catch (const sys::system_error& e) {
      push_failed = true;
      ldpp_dout(dpp, -1) << "RGWDataChangesLog::synthesize_entries(): Unable to "
			 << "parse Bucketgen key: " << key << "Got exception: "
			 << e.what() << dendl;
    }
  }
  // semaphores are not per zonegroup, push entries to all zonegroups in be
  const auto& zg_ids = get_zonegroup_ids(be->per_zonegroup);
  for (auto it = zg_ids.begin(); it != zg_ids.end(); ++it) {
    try {
      RGWDataChangesBE::entries batch_to_push =
          (std::next(it) != zg_ids.end()) ? batch : std::move(batch);
      co_await be->push(dpp, *it, index, std::move(batch_to_push));
    } catch (const std::exception& e) {
      push_failed = true;
      ldpp_dout(dpp, 5) << "RGWDataChangesLog::synthesize_entries(): Backend push "
		        << "to zg " << *it << " failed with exception: "
		        << e.what() << dendl;
    }
  }
  co_return !push_failed;
}

asio::awaitable<bool>
RGWDataChangesLog::gather_working_sets(
  const DoutPrefixProvider* dpp,
  int shard,
  bc::flat_map<std::string, uint64_t>& semcount)
{
  buffer::list bl;
  recovery_check rc;
  rc.shard = shard;
  rc.keys.reserve(semcount.size());
  for (const auto& [key, count] : semcount) {
    rc.keys.emplace_back(key);
  }
  encode(rc, bl);
  auto [reply_map, missed_set] = co_await rados->notify(
      get_recover_lock_oid(), loc, bl, 60s, asio::use_awaitable);
  // If we didn't get an answer from someone, don't decrement anything.
  if (!missed_set.empty()) {
    ldpp_dout(dpp, 5) << "RGWDataChangesLog::gather_working_sets(): Missed responses: "
		      << missed_set << dendl;
    co_return false;
  }
  for (const auto& [source, reply] : reply_map) {
    recovery_reply counts;
    try {
      decode(counts, reply);
    } catch (const std::exception& e) {
      ldpp_dout(dpp, -1)
	<< "RGWDataChangesLog::gather_working_sets(): Failed decoding reply from: "
	<< source << dendl;
      co_return false;
    }
    if (rc.keys.size() != counts.reply_set.size()) {
      ldpp_dout(dpp, -1)
	<< "RGWDataChangesLog::gather_working_sets(): reply set does not match: "
	<< source << dendl;
      co_return false;
    }
    for (auto i = 0u; i < rc.keys.size(); ++i) {
      const auto& key = rc.keys[i];
      const auto& count = counts.reply_set[i];
      auto iter = semcount.find(key);
      if (iter == semcount.end()) {
	continue;
      }
      if (iter->second <= count) {
	semcount.erase(iter);
      } else {
	(iter->second) -= count;
      }
    }
  }
  co_return true;
}

asio::awaitable<void>
RGWDataChangesLog::decrement_sems(
  int index,
  ceph::mono_time fetch_time,
  bc::flat_map<std::string, uint64_t>&& semcount)
{
  namespace sem_set = neorados::cls::sem_set;
  while (!semcount.empty()) {
    bc::flat_set<std::string> batch;
    for (auto j = 0u; j < sem_max_keys && !semcount.empty(); ++j) {
      auto iter = std::begin(semcount);
      batch.insert(iter->first);
      semcount.erase(std::move(iter));
    }
    auto grace = ((ceph::mono_clock::now() - fetch_time) * 4) / 3;
    co_await rados->execute(
      get_sem_set_oid(index), loc, neorados::WriteOp{}.exec(
	ss::decrement(std::move(batch), grace)),
      asio::use_awaitable);
  }
}

asio::awaitable<void>
RGWDataChangesLog::recover_shard(const DoutPrefixProvider* dpp, int index)
{
  std::string cursor;
  do {
    bc::flat_map<std::string, uint64_t> semcount;

    auto fetch_time = ceph::mono_clock::now();
    // Gather entries in the shard
    std::tie(semcount, cursor) = co_await read_sems(index, std::move(cursor));
    // If we have none, no point doing the rest
    if (semcount.empty()) {
      break;
    }

    // Synthesize entries to push
    auto pushed = co_await synthesize_entries(dpp, index, semcount);
    if (!pushed) {
      // If pushing failed, don't decrement any semaphores
      ldpp_dout(dpp, 5) << "RGWDataChangesLog::recover_shard(): Pushing shard "
			<< index << " failed, skipping decrement" << dendl;
      continue;
    }

    // Check with other running RGWs, make sure not to decrement
    // anything they have in flight. This doesn't cause an issue for
    // partial upgrades, since older versions won't be using the
    // semaphores at all.
    auto notified = co_await gather_working_sets(dpp, index, semcount);
    if (!notified) {
      ldpp_dout(dpp, 5) << "RGWDataChangesLog::recover_shard(): Gathering "
			<< "working sets for shard " << index
			<< "failed, skipping decrement" << dendl;
      continue;
    }
    co_await decrement_sems(index, fetch_time, std::move(semcount));
  } while (!cursor.empty());
  co_return;
}

asio::awaitable<void> RGWDataChangesLog::recover(
  const DoutPrefixProvider* dpp)
{
  co_await asio::co_spawn(
    recovery_strand,
    [this](const DoutPrefixProvider* dpp)-> asio::awaitable<void, strand_t> {
      auto ex = recovery_strand;
      auto group = async::spawn_group{ex, static_cast<size_t>(num_shards)};
      for (auto i = 0; i < num_shards; ++i) {
	boost::asio::co_spawn(ex, recover_shard(dpp, i), group);
      }
      co_await group.wait();
    }(dpp),
    asio::use_awaitable);

  std::unique_lock l(lock);
  last_recovery = ceph::mono_clock::now();
  l.unlock();
}

asio::awaitable<void>
RGWDataChangesLog::admin_sem_list(std::optional<int> req_shard,
				  std::uint64_t max_entries,
				  std::string marker,
				  std::ostream& m,
				  ceph::Formatter& formatter)
{
  int shard = req_shard.value_or(0);
  std::string keptmark;

  if (!marker.empty()) {
    // Signal caught by radosgw-admin
    BucketGen bg{marker};
    auto index = choose_shard_id(bg.shard);
    if (req_shard && *req_shard != index) {
      throw sys::system_error{
	EINVAL, sys::generic_category(),
	fmt::format("Requested shard {} but marker is for shard {}",
		    shard, index)};
    }
  }
  bc::flat_map<std::string, std::uint64_t> entries;
  std::uint64_t count = 0;
  bool begin_next = false;
  // So the marker traverses between shards if the last entry in the
  // shard is the last needed for max_entries
  std::string mkeep;
  entries.reserve(sem_max_keys);
  formatter.open_object_section("semaphores");
  formatter.open_array_section("entries");
  while ((max_entries == 0 || (count < max_entries)) && shard < num_shards) {
    entries.clear();
    try {
      if (begin_next) {
	marker.clear();
	begin_next = false;
      }
      co_await rados->execute(get_sem_set_oid(shard), loc,
			      neorados::ReadOp{}.
			      exec(ss::list(std::min(max_entries - count,
						     sem_max_keys),
					    marker,
					    &entries, &marker)),
	nullptr, asio::use_awaitable);
      if (!marker.empty()) {
	mkeep = marker;
      }
    } catch (const sys::system_error& e) {
      if (e.code() == sys::errc::no_such_file_or_directory ||
          e.code() == ceph::buffer::errc::end_of_buffer) {
	if (!req_shard) {
	  begin_next = true;
	  ++shard;
	  continue;
	} else {
	  break;
	}
      } else {
	throw;
      }
    }
    for (auto i = entries.cbegin(); i != entries.cend(); ++i) {
      const auto& [k, v] = *i;
      formatter.open_object_section("semaphore");
      formatter.dump_string("key", k);
      formatter.dump_unsigned("count", v);
      formatter.close_section();
      ++count;
    }
    formatter.flush(m);
    if (marker.empty()) {
      if (!entries.empty()) {
	mkeep = (entries.cend() - 1)->first;
      }
      if (!req_shard) {
	++shard;
      } else {
	break;
      }
    }
  }
  if (shard < num_shards && !req_shard && count == max_entries) {
    marker = std::move(mkeep);
  }
  formatter.close_section();
  formatter.dump_string("marker", marker);
  formatter.close_section();
  formatter.flush(m);
  co_return;
}

asio::awaitable<void>
RGWDataChangesLog::admin_sem_reset(std::string_view marker,
				   std::uint64_t count)
{
  // Exceptions here are caught by radosgw-admin
  BucketGen bg{marker};
  unsigned index = choose_shard_id(bg.shard);
  auto wop = neorados::WriteOp{}.exec(ss::reset(std::string(marker), count));
  co_await rados->execute(get_sem_set_oid(index), loc,
			  std::move(wop), asio::use_awaitable);
}

void RGWDataChangesLogInfo::dump(Formatter *f) const
{
  encode_json("marker", marker, f);
  utime_t ut(last_update);
  encode_json("last_update", ut, f);
}

void RGWDataChangesLogInfo::decode_json(JSONObj *obj)
{
  JSONDecoder::decode_json("marker", marker, obj);
  JSONDecoder::decode_json("last_update", last_update, obj);
}
