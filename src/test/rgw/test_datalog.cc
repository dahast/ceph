// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2019 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "rgw_datalog.h"

#include <string_view>

#include <boost/asio/awaitable.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <boost/system/errc.hpp>
#include <boost/system/error_code.hpp>

#include <fmt/format.h>

#include "include/neorados/RADOS.hpp"

#include "neorados/cls/sem_set.h"

#include "test/neorados/common_tests.h"

#include "gtest/gtest.h"

namespace asio = boost::asio;
namespace ss = neorados::cls::sem_set;

using neorados::WriteOp;

class DataLogTestBase : public NeoRadosTest {
private:
  virtual asio::awaitable<std::unique_ptr<RGWDataChangesLog>>
  create_datalog() = 0;

protected:

  std::unique_ptr<RGWDataChangesLog> datalog;

  asio::awaitable<void>
  read_all_sems(int index,
		bc::flat_map<std::string, uint64_t>* out) {
    std::string cursor;
    do {
      try {
	co_await rados().execute(
	  datalog->get_sem_set_oid(index), datalog->loc,
	  neorados::ReadOp{}.exec(ss::list(datalog->sem_max_keys, cursor, out,
					   &cursor)),
	  nullptr, asio::use_awaitable);
      } catch (const sys::system_error& e) {
	if (e.code() == sys::errc::no_such_file_or_directory) {
	  break;
	} else {
	  throw;
	}
      }
    } while (!cursor.empty());
    co_return;
  }

  asio::awaitable<bc::flat_map<std::string, uint64_t>>
  read_all_sems_all_shards() {
    bc::flat_map<std::string, uint64_t> all_sems;

    for (auto i = 0; i < datalog->num_shards; ++i) {
      co_await read_all_sems(i, &all_sems);
    }
    co_return std::move(all_sems);
  }

  asio::awaitable<bc::flat_map<BucketGen, uint64_t>>
  read_all_log(const DoutPrefixProvider* dpp,
               const std::string& zg_id = "") {
    bc::flat_map<BucketGen, uint64_t> all_keys;

    RGWDataChangesLogMarker marker;
    do {
      std::vector<rgw_data_change_log_entry> entries;
      std::tie(entries, marker, std::ignore) =
	co_await datalog->list_entries(dpp, zg_id, 1'000,
				       std::move(marker));
      for (const auto& entry : entries) {
	auto key = fmt::format("{}:{}", entry.entry.key, entry.entry.gen);
	all_keys[BucketGen{key}] += 1;
      }
    } while (marker);
    co_return std::move(all_keys);
  }

  asio::awaitable<void> add_entry(const DoutPrefixProvider* dpp,
                                  const BucketGen& bg) {
    RGWBucketInfo bi;
    bi.bucket = bg.shard.bucket;
    rgw::bucket_log_layout_generation gen;
    gen.gen = bg.gen;
    co_await datalog->add_entry(dpp, bi, gen, bg.shard.shard_id);
    co_return;
  }

  auto renew_entries(const DoutPrefixProvider* dpp) {
    return datalog->renew_entries(dpp);
  }

  auto oid(const BucketGen& bg) {
    return datalog->get_oid(0, "", datalog->choose_shard_id(bg.shard));
  }

  auto sem_set_oid(const BucketGen& bg) {
    return datalog->get_sem_set_oid(datalog->choose_shard_id(bg.shard));
  }

  auto loc() {
    return datalog->loc;
  }

  auto recover(const DoutPrefixProvider* dpp) {
    return datalog->recover(dpp);
  }

  void add_to_cur_cycle(const BucketGen& bg) {
    std::unique_lock l(datalog->lock);
    datalog->cur_cycle[bg].insert("");
  }

  void add_to_semaphores(const BucketGen& bg) {
    std::unique_lock l(datalog->lock);
    datalog->semaphores[datalog->choose_shard_id(bg.shard)].insert(bg.get_key());
  }

public:

  boost::asio::awaitable<void> CoSetUp() override {
    co_await NeoRadosTest::CoSetUp();
    add_io_threads(2);
    datalog = co_await create_datalog();
    co_return;
  }

  ~DataLogTestBase() override = default;

  boost::asio::awaitable<void> CoTearDown() override {
    co_await datalog->async_shutdown();
    co_await NeoRadosTest::CoTearDown();
    co_return;
  }
};

class DataLogTest : public DataLogTestBase {
private:
  asio::awaitable<std::unique_ptr<RGWDataChangesLog>> create_datalog() override {
    auto datalog = std::make_unique<RGWDataChangesLog>(rados().cct(), true,
						       rados());
    std::string zg_id = "testzg";
    std::vector<std::string> zg_ids{zg_id};
    co_await datalog->start(dpp(), rgw_pool(pool_name()),
      std::move(zg_id), std::move(zg_ids), false,
      false, true, false);
    co_return std::move(datalog);
  }
};

class DataLogWatchless : public DataLogTestBase {
private:
  asio::awaitable<std::unique_ptr<RGWDataChangesLog>> create_datalog() override {
    auto datalog = std::make_unique<RGWDataChangesLog>(rados().cct(), true,
						       rados());
    std::string zg_id = "testzg";
    std::vector<std::string> zg_ids{zg_id};
    co_await datalog->start(dpp(), rgw_pool(pool_name()),
      std::move(zg_id), std::move(zg_ids), false,
      false, false, false);
    co_return std::move(datalog);
  }
};

class DataLogBulky : public DataLogTestBase {
private:
  asio::awaitable<std::unique_ptr<RGWDataChangesLog>> create_datalog() override {
    // Decrease max push/list and force everything into one shard so we
    // can test iterated increment/decrement/list code.
    auto datalog = std::make_unique<RGWDataChangesLog>(rados().cct(), true,
						       rados(), 1, 7);
    std::string zg_id = "testzg";
    std::vector<std::string> zg_ids{zg_id};
    co_await datalog->start(dpp(), rgw_pool(pool_name()),
      std::move(zg_id), std::move(zg_ids), false,
      false, true, false);
    co_return std::move(datalog);
  }
};

// Two zonegroups, per_zonegroup=true.  Used to test per-zonegroup isolation.
class DataLogPerZonegroup : public DataLogTestBase {
private:
  asio::awaitable<std::unique_ptr<RGWDataChangesLog>> create_datalog() override {
    auto datalog = std::make_unique<RGWDataChangesLog>(rados().cct(), true,
						       rados());
    std::string own_zg = "zgA";
    std::vector<std::string> zg_ids{"zgA", "zgB"};
    co_await datalog->start(dpp(), rgw_pool(pool_name()),
      std::move(own_zg), std::move(zg_ids), /*per_zonegroup=*/true,
      false, true, false);
    co_return std::move(datalog);
  }

};



const std::vector<BucketGen> ref{
  {{{"fred", "foo"}, 32}, 3},
  {{{"fred", "foo"}, 32}, 0},
  {{{"fred", "foo"}, 13}, 0},
  {{{"", "bar"}, 13}, 0},
  {{{"", "bar", "zardoz"}, 11}, 0}};

const auto bulky =
  []() {
    std::vector<BucketGen> ref;
    for (auto i = 0; i < 30; ++i) {
      ref.push_back({{{"", fmt::format("bucket{}", i)}, i}, 0});
      ref.push_back({{{fmt::format("tenant{}", i),
	               fmt::format("bucket{}", i)}, i}, 0});
      ref.push_back({{{fmt::format("tenant{}", i),
	               fmt::format("bucket{}", i),
	               fmt::format("instance{}", i)}, i}, 0});
    }
    return ref;
  }();

TEST(DataLogBG, TestRoundTrip) {
  for (const auto& bg : ref) {
    ASSERT_EQ(bg, BucketGen{bg.get_key()});
  }
}

CORO_TEST_F(DataLog, TestSem, DataLogTest) {
  for (const auto& bg : ref) {
    co_await add_entry(dpp(), bg);
    // Second send adds it to working set and creates the semaphore
    co_await add_entry(dpp(), bg);
    // Third should *not* increment the semaphore again.
    co_await add_entry(dpp(), bg);
  }
  auto sems = co_await read_all_sems_all_shards();
  for (const auto& bg : ref) {
    EXPECT_TRUE(sems.contains(bg.get_key()));
    EXPECT_EQ(1, sems[bg.get_key()]);
  }
  co_await renew_entries(dpp());
  sems.clear();
  sems = co_await read_all_sems_all_shards();
  EXPECT_TRUE(sems.empty());
  const auto log_entries = co_await read_all_log(dpp());
  for (const auto& bg : ref) {
    EXPECT_TRUE(log_entries.contains(bg));
  }
  co_return;
}

CORO_TEST_F(DataLog, SimpleRecovery, DataLogTest) {
  for (const auto& bg : ref) {
    co_await rados().execute(sem_set_oid(bg), loc(),
			     WriteOp{}.exec(ss::increment(bg.get_key())),
			     asio::use_awaitable);
  }
  co_await recover(dpp());
  auto sems = co_await read_all_sems_all_shards();
  EXPECT_TRUE(sems.empty());

  auto log_entries = co_await read_all_log(dpp());
  for (const auto& bg : ref) {
    EXPECT_TRUE(log_entries.contains(bg));
  }

  co_return;
}

CORO_TEST_F(DataLog, CycleRecovery, DataLogTest) {
  for (const auto& bg : ref) {
    co_await rados().execute(sem_set_oid(bg), loc(),
			     WriteOp{}.exec(ss::increment(bg.get_key())),
			     asio::use_awaitable);
  }
  add_to_cur_cycle(ref[0]);
  add_to_cur_cycle(ref[1]);
  co_await recover(dpp());
  auto sems = co_await read_all_sems_all_shards();
  for (const auto& bg : {ref[0], ref[1]}) {
    EXPECT_TRUE(sems.contains(bg.get_key()));
  }
  for (const auto& bg : {ref[2], ref[3], ref[4]}) {
    EXPECT_FALSE(sems.contains(bg.get_key()));
  }

  auto log_entries = co_await read_all_log(dpp());
  for (const auto& bg : ref) {
    EXPECT_TRUE(log_entries.contains(bg));
  }

  co_return;
}

CORO_TEST_F(DataLog, SemaphoresRecovery, DataLogTest) {
  for (const auto& bg : ref) {
    co_await rados().execute(sem_set_oid(bg), loc(),
			     WriteOp{}.exec(ss::increment(bg.get_key())),
			     asio::use_awaitable);
  }
  add_to_semaphores(ref[0]);
  add_to_semaphores(ref[1]);
  co_await recover(dpp());
  auto sems = co_await read_all_sems_all_shards();
  for (const auto& bg : {ref[0], ref[1]}) {
    EXPECT_TRUE(sems.contains(bg.get_key()));
  }
  for (const auto& bg : {ref[2], ref[3], ref[4]}) {
    EXPECT_FALSE(sems.contains(bg.get_key()));
  }

  const auto log_entries = co_await read_all_log(dpp());
  for (const auto& bg : ref) {
    EXPECT_EQ(1, log_entries.at(bg));
  }

  co_return;
}

CORO_TEST_F(DataLogWatchless, NotWatching, DataLogWatchless) {
  for (const auto& bg : ref) {
    co_await add_entry(dpp(), bg);
    // With watch down, we should bypass the data window and get two entries
    co_await add_entry(dpp(), bg);
  }
  auto sems = co_await read_all_sems_all_shards();
  EXPECT_TRUE(sems.empty());
  const auto log_entries = co_await read_all_log(dpp());
  for (const auto& bg : ref) {
    EXPECT_EQ(2, log_entries.at(bg));
  }
  co_return;
}

CORO_TEST_F(DataLogBulky, TestSemBulky, DataLogBulky) {
  for (const auto& bg : bulky) {
    co_await add_entry(dpp(), bg);
    // Second send adds it to working set and creates the semaphore
    co_await add_entry(dpp(), bg);
  }
  auto sems = co_await read_all_sems_all_shards();
  for (const auto& bg : bulky) {
    EXPECT_TRUE(sems.contains(bg.get_key()));
    EXPECT_EQ(1, sems[bg.get_key()]);
  }
  co_await renew_entries(dpp());
  sems.clear();
  sems = co_await read_all_sems_all_shards();
  EXPECT_TRUE(sems.empty());
  const auto log_entries = co_await read_all_log(dpp());
  for (const auto& bg : bulky) {
    EXPECT_TRUE(log_entries.contains(bg));
  }
  co_return;
}

CORO_TEST_F(DataLogBulky, BulkyRecovery, DataLogBulky) {
  for (const auto& bg : bulky) {
    co_await rados().execute(sem_set_oid(bg), loc(),
			     WriteOp{}.exec(ss::increment(bg.get_key())),
			     asio::use_awaitable);
  }
  co_await recover(dpp());
  auto sems = co_await read_all_sems_all_shards();
  EXPECT_TRUE(sems.empty());

  auto log_entries = co_await read_all_log(dpp());
  for (const auto& bg : bulky) {
    EXPECT_TRUE(log_entries.contains(bg));
  }

  co_return;
}

CORO_TEST_F(DataLogBulky, BulkyCycleRecovery, DataLogBulky) {
  for (const auto& bg : bulky) {
    co_await rados().execute(sem_set_oid(bg), loc(),
			     WriteOp{}.exec(ss::increment(bg.get_key())),
			     asio::use_awaitable);
  }
  for (auto i = 0u; i < bulky.size(); ++i) {
    if (i % 2 == 0) {
      add_to_cur_cycle(bulky[i]);
    }
  }
  co_await recover(dpp());
  auto sems = co_await read_all_sems_all_shards();
  for (auto i = 0u; i < bulky.size(); ++i) {
    if (i % 2 == 0) {
      EXPECT_TRUE(sems.contains(bulky[i].get_key()));
    } else {
      EXPECT_FALSE(sems.contains(bulky[i].get_key()));
    }
  }

  auto log_entries = co_await read_all_log(dpp());
  for (const auto& bg : bulky) {
    EXPECT_TRUE(log_entries.contains(bg));
  }
  co_return;
}

// trim_entries with max_marker() must not crash on a single-generation cluster.
CORO_TEST_F(DataLogBulky, TrimWithMaxMarker, DataLogBulky) {
  for (const auto& bg : bulky) {
    co_await add_entry(dpp(), bg);
  }
  co_await renew_entries(dpp());
  co_await datalog->trim_entries(dpp(), 0, datalog->max_marker());
  co_return;
}

// Entries written to a later generation must appear in list_entries even when
// an earlier generation on the same shard was exhausted first.
// Uses DataLogWatchless so every add_entry push goes straight to the log
// without the dedup window, making the generation boundary clear-cut.
CORO_TEST_F(DataLogWatchless, MultiGenListing, DataLogWatchless) {
  const std::vector<BucketGen> gen0_entries{
    {{{"", "bucket-a"}, 0}, 0},
    {{{"", "bucket-b"}, 0}, 0},
  };
  const std::vector<BucketGen> gen1_entries{
    {{{"", "bucket-c"}, 0}, 0},
    {{{"", "bucket-d"}, 0}, 0},
  };

  // Write entries to gen 0 (fifo).
  for (const auto& bg : gen0_entries) {
    co_await add_entry(dpp(), bg);
  }

  // Open gen 1 (omap).  All subsequent writes go to gen 1.
  co_await datalog->change_format(dpp(), log_type::omap);

  // Write entries to gen 1.
  for (const auto& bg : gen1_entries) {
    co_await add_entry(dpp(), bg);
  }

  // Both generations must be visible in a single full listing.
  auto all = co_await read_all_log(dpp());
  for (const auto& bg : gen0_entries) {
    EXPECT_TRUE(all.contains(bg)) << "gen 0 missing: " << bg.get_key();
  }
  for (const auto& bg : gen1_entries) {
    EXPECT_TRUE(all.contains(bg)) << "gen 1 missing: " << bg.get_key();
  }
  co_return;
}

CORO_TEST_F(DataLogBulky, BulkySemaphoresRecovery, DataLogBulky) {
  for (const auto& bg : bulky) {
    co_await rados().execute(sem_set_oid(bg), loc(),
			     WriteOp{}.exec(ss::increment(bg.get_key())),
			     asio::use_awaitable);
  }
  for (auto i = 0u; i < bulky.size(); ++i) {
    if (i % 2 == 0) {
      add_to_semaphores(bulky[i]);
    }
  }
  co_await recover(dpp());
  auto sems = co_await read_all_sems_all_shards();
  for (auto i = 0u; i < bulky.size(); ++i) {
    if (i % 2 == 0) {
      EXPECT_TRUE(sems.contains(bulky[i].get_key()));
    } else {
      EXPECT_FALSE(sems.contains(bulky[i].get_key()));
    }
  }

  auto log_entries = co_await read_all_log(dpp());
  for (const auto& bg : bulky) {
    EXPECT_TRUE(log_entries.contains(bg));
  }
  co_return;
}

// ---------------------------------------------------------------------------
// Unit tests for OID naming helpers (use the pre-created datalog fixture)
// ---------------------------------------------------------------------------

CORO_TEST_F(DataLog, OidGen0HasNoGenerationSuffix, DataLogTest) {
  EXPECT_EQ("data_log.0", datalog->get_oid(0, "", 0));
  EXPECT_EQ("data_log.3", datalog->get_oid(0, "", 3));
  // zg_id is ignored for gen 0
  EXPECT_EQ("data_log.2", datalog->get_oid(0, "somezg", 2));
  co_return;
}

CORO_TEST_F(DataLog, OidGen1NoZonegroupId, DataLogTest) {
  EXPECT_EQ("data_log@G1.0", datalog->get_oid(1, "", 0));
  EXPECT_EQ("data_log@G1.7", datalog->get_oid(1, "", 7));
  co_return;
}

CORO_TEST_F(DataLog, OidGen1WithZonegroupId, DataLogTest) {
  EXPECT_EQ("data_log@G1@ZzgA.0", datalog->get_oid(1, "zgA", 0));
  EXPECT_EQ("data_log@G2@ZzgB.5", datalog->get_oid(2, "zgB", 5));
  co_return;
}

CORO_TEST_F(DataLog, OidListPrefix, DataLogTest) {
  EXPECT_EQ("data_log@G3@Z", datalog->get_oid_list_prefix(3));
  EXPECT_EQ("data_log@G1@Z", datalog->get_oid_list_prefix(1));
  co_return;
}

// ---------------------------------------------------------------------------
// effective_zg_id: minimal mock backend carrying only the per_zonegroup flag.
// ---------------------------------------------------------------------------

namespace {
struct MockBE : public RGWDataChangesBE {
  using RGWDataChangesBE::RGWDataChangesBE;
  void prepare(ceph::real_time, const std::string&,
	       ceph::buffer::list&&, entries&) override {}
  asio::awaitable<void>
  push(const DoutPrefixProvider*, const std::string&, int,
       entries&&) override { co_return; }
  void push(const DoutPrefixProvider*, const std::string&, int,
	    ceph::real_time, const std::string&,
	    ceph::buffer::list&&, asio::yield_context) override {}
  asio::awaitable<std::tuple<std::span<rgw_data_change_log_entry>, std::string>>
  list(const DoutPrefixProvider*, const std::string&, int,
       std::span<rgw_data_change_log_entry> e, std::string) override {
    co_return std::make_tuple(e.first(0), std::string{});
  }
  asio::awaitable<RGWDataChangesLogInfo>
  get_info(const DoutPrefixProvider*, const std::string&, int) override {
    co_return RGWDataChangesLogInfo{};
  }
  asio::awaitable<void>
  trim(const DoutPrefixProvider*, const std::string&, int,
       std::string_view) override { co_return; }
  std::string_view max_marker() const override { return ""; }
  asio::awaitable<bool> is_empty(const DoutPrefixProvider*) override {
    co_return true;
  }
};
} // anonymous namespace

CORO_TEST_F(DataLog, EffectiveZgIdNotPerZonegroup, DataLogTest) {
  // When be.per_zonegroup==false, effective_zg_id always returns "".
  MockBE be{rados(), loc(), *datalog, 0, /*per_zonegroup=*/false};
  EXPECT_EQ("", datalog->effective_zg_id(be, ""));
  EXPECT_EQ("", datalog->effective_zg_id(be, "zgA"));
  co_return;
}

CORO_TEST_F(DataLog, EffectiveZgIdPerZonegroupEmpty, DataLogTest) {
  // When be.per_zonegroup==true and zg_id=="" → fall back to own_zonegroup_id.
  MockBE be{rados(), loc(), *datalog, 1, /*per_zonegroup=*/true};
  EXPECT_EQ(datalog->get_own_zonegroup_id(),
	    datalog->effective_zg_id(be, ""));
  co_return;
}

CORO_TEST_F(DataLog, EffectiveZgIdPerZonegroupExplicit, DataLogTest) {
  // When be.per_zonegroup==true and an explicit zg_id is given, return it.
  MockBE be{rados(), loc(), *datalog, 1, /*per_zonegroup=*/true};
  EXPECT_EQ("zgB", datalog->effective_zg_id(be, "zgB"));
  co_return;
}

// ---------------------------------------------------------------------------
// Per-zonegroup isolation tests
// ---------------------------------------------------------------------------

CORO_TEST_F(DataLogPerZonegroup, IsolationBothZonegroupsSeeEntries,
	    DataLogPerZonegroup) {
  for (const auto& bg : ref) {
    co_await add_entry(dpp(), bg);
    co_await add_entry(dpp(), bg);
  }
  co_await renew_entries(dpp());

  // add_entry fans out to all registered zonegroups, so both must see entries.
  auto za = co_await read_all_log(dpp(), "zgA");
  auto zb = co_await read_all_log(dpp(), "zgB");
  for (const auto& bg : ref) {
    EXPECT_TRUE(za.contains(bg)) << "zgA missing " << bg.get_key();
    EXPECT_TRUE(zb.contains(bg)) << "zgB missing " << bg.get_key();
  }
  co_return;
}

CORO_TEST_F(DataLogPerZonegroup, EmptyZgIdFallsBackToOwnZonegroup,
	    DataLogPerZonegroup) {
  // list_entries with zg_id=="" must fall back to own_zonegroup_id ("zgA").
  for (const auto& bg : ref) {
    co_await add_entry(dpp(), bg);
    co_await add_entry(dpp(), bg);
  }
  co_await renew_entries(dpp());

  auto empty_zg = co_await read_all_log(dpp(), "");
  auto own_zg   = co_await read_all_log(dpp(), "zgA");
  EXPECT_EQ(empty_zg, own_zg);
  co_return;
}
