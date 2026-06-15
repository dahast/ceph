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

#include "rgw_log_backing.h"

#include <string_view>

#include <boost/asio/awaitable.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <boost/system/errc.hpp>
#include <boost/system/error_code.hpp>

#include <fmt/format.h>

#include "include/rados/librados.hpp"
#include "include/neorados/RADOS.hpp"

#include "common/ceph_time.h"

#include "neorados/cls/fifo.h"
#include "neorados/cls/log.h"

#include "test/neorados/common_tests.h"

#include "gtest/gtest.h"

namespace asio = boost::asio;
namespace buffer = ceph::buffer;

namespace fifo = neorados::cls::fifo;
namespace logn = neorados::cls::log;

namespace {
inline constexpr int SHARDS = 3;
std::string get_oid(uint64_t gen_id, const std::string& zg_id, int i) {
  return (gen_id > 0 ?
	  (zg_id.empty() ?
	    fmt::format("shard@G{}.{}", gen_id, i) :
	    fmt::format("shard@G{}@Z{}.{}", gen_id, zg_id, i)) :
	  fmt::format("shard.{}", i));
}

gen_oids get_oids(uint64_t gen_id, const std::vector<std::string>& zg_ids) {
  gen_oids oids;
  for (const std::string& zg_id : zg_ids) {
    for (int i = 0; i < SHARDS; ++i) {
      auto oid = get_oid(gen_id, zg_id, i);
      if (gen_id == 0 && i == 0)
        oids[oid] = remove_action::clear;
      else
        oids[oid] = remove_action::remove;
    }
  }
  return oids;
}

gen_oids get_gen0_oids() {
  std::vector<std::string> zg_ids{""};
  return get_oids(0, zg_ids);
}

asio::awaitable<void> make_omap(neorados::RADOS& rados,
				const neorados::IOContext& loc) {
  for (int i = 0; i < SHARDS; ++i) {
    using ceph::encode;
    neorados::WriteOp op;
    buffer::list bl;
    encode(i, bl);
    op.exec(logn::add(ceph::real_clock::now(), {}, "meow", std::move(bl)));
    co_await rados.execute(get_oid(0, "", i), loc, std::move(op),
			   asio::use_awaitable);
  }
  co_return;
}

asio::awaitable<void> make_fifo(const DoutPrefixProvider* dpp,
				neorados::RADOS& rados,
                                const neorados::IOContext& loc) {
  for (int i = 0; i < SHARDS; ++i) {
    auto fifo = co_await fifo::FIFO::create(dpp, rados, get_oid(0, "", i), loc,
					    asio::use_awaitable);
    EXPECT_TRUE(fifo);
  }
}
}

CORO_TEST_F(LogBacking, TestOmap, NeoRadosTest)
{
  co_await make_omap(rados(), pool());
  auto stat = co_await log_backing_type(
    dpp(), rados(), pool(), get_gen0_oids(), log_type::fifo);
  EXPECT_EQ(log_type::omap, stat);
}


CORO_TEST_F(LogBacking, TestOmapEmpty, NeoRadosTest)
{
  auto stat = co_await log_backing_type(
    dpp(), rados(), pool(), get_gen0_oids(), log_type::omap);
  EXPECT_EQ(log_type::omap, stat);
}

CORO_TEST_F(LogBacking, TestFIFO, NeoRadosTest)
{
  co_await make_fifo(dpp(), rados(), pool());
  auto stat = co_await log_backing_type(
    dpp(), rados(), pool(), get_gen0_oids(), log_type::fifo);
  EXPECT_EQ(log_type::fifo, stat);
}

CORO_TEST_F(LogBacking, TestFIFOEmpty, NeoRadosTest)
{
  auto stat = co_await log_backing_type(
    dpp(), rados(), pool(), get_gen0_oids(), log_type::fifo);
  EXPECT_EQ(log_type::fifo, stat);
}

TEST(LogbackGeneration, EncodeDecodePerZonegroupTrue) {
  logback_generation g;
  g.gen_id = 5;
  g.type = log_type::fifo;
  g.per_zonegroup = true;

  ceph::buffer::list bl;
  g.encode(bl);

  logback_generation g2;
  auto bi = bl.cbegin();
  g2.decode(bi);

  EXPECT_EQ(g.gen_id, g2.gen_id);
  EXPECT_EQ(g.type, g2.type);
  EXPECT_EQ(g.per_zonegroup, g2.per_zonegroup);
}

TEST(LogbackGeneration, EncodeDecodePerZonegroupFalse) {
  logback_generation g;
  g.gen_id = 3;
  g.type = log_type::omap;
  g.per_zonegroup = false;

  ceph::buffer::list bl;
  g.encode(bl);

  logback_generation g2;
  auto bi = bl.cbegin();
  g2.decode(bi);

  EXPECT_EQ(g.gen_id, g2.gen_id);
  EXPECT_EQ(g.type, g2.type);
  EXPECT_FALSE(g2.per_zonegroup);
}

TEST(LogbackGeneration, BackwardCompatV1DecodesAsNotPerZonegroup) {
  // Encode a struct_v 1 payload (no per_zonegroup field) and verify
  // that decoding it yields per_zonegroup=false.
  logback_generation g;
  g.gen_id = 7;
  g.type = log_type::fifo;
  g.per_zonegroup = true; // will be ignored — we'll strip it by hand

  // Manually encode a struct_v 1 message (same layout as the old code):
  // ENCODE_START(1, 1, bl) encodes gen_id, type, pruned, then ENCODE_FINISH.
  ceph::buffer::list bl;
  {
    using ceph::encode;
    ENCODE_START(1, 1, bl);
    encode(g.gen_id, bl);
    encode(g.type, bl);
    encode(g.pruned, bl);
    ENCODE_FINISH(bl);
  }

  logback_generation g2;
  auto bi = bl.cbegin();
  g2.decode(bi);

  EXPECT_EQ(7u, g2.gen_id);
  EXPECT_EQ(log_type::fifo, g2.type);
  EXPECT_FALSE(g2.per_zonegroup);
}

TEST(CursorGen, RoundTrip) {
  const std::string_view pcurs = "fded";
  {
    auto gc = gencursor(0, pcurs);
    ASSERT_EQ(pcurs, gc);
    auto [gen, cursor] = cursorgen(gc);
    ASSERT_EQ(0, gen);
    ASSERT_EQ(pcurs, cursor);
  }
  {
    auto gc = gencursor(53, pcurs);
    ASSERT_NE(pcurs, gc);
    auto [gen, cursor] = cursorgen(gc);
    ASSERT_EQ(53, gen);
    ASSERT_EQ(pcurs, cursor);
  }
}

class generations final : public logback_generations {
public:

  entries_t got_entries;
  std::optional<uint64_t> tail;

  using logback_generations::logback_generations;

  asio::awaitable<gen_oids> get_gen_oids(const logback_generation& g) override {
    std::vector<std::string> zg_ids{""};
    co_return get_oids(g.gen_id, zg_ids);
  }

  void handle_init(entries_t e) override {
    got_entries = e;
  }

  void handle_new_gens(entries_t e) override {
    got_entries = e;
  }

  void handle_empty_to(uint64_t new_tail) override {
    tail = new_tail;
  }
};

CORO_TEST_F(LogBacking, GenerationSingle, NeoRadosTest) {
  // logback_generations::operator() and shutdown() use async::use_blocked
  // and need addtional threads for the completion
  add_io_threads(2);
  auto lg = co_await logback_generations::init<generations>(
    dpp(), rados(), "foobar", pool(), log_type::fifo);

  EXPECT_FALSE(lg->got_entries.empty());
  EXPECT_EQ(0, lg->got_entries.begin()->first);

  EXPECT_EQ(0, lg->got_entries[0].gen_id);
  EXPECT_EQ(log_type::fifo, lg->got_entries[0].type);
  EXPECT_FALSE(lg->got_entries[0].pruned);

  EXPECT_THROW({
      co_await lg->empty_to(dpp(), 0);
    }, sys::system_error);

  lg->shutdown();
  lg.reset();

  lg = co_await logback_generations::init<generations>(
    dpp(), rados(), "foobar", pool(), log_type::fifo);

  EXPECT_EQ(0, lg->got_entries.begin()->first);

  EXPECT_EQ(0, lg->got_entries[0].gen_id);
  EXPECT_EQ(log_type::fifo, lg->got_entries[0].type);
  EXPECT_FALSE(lg->got_entries[0].pruned);

  lg->got_entries.clear();

  co_await lg->new_backing(dpp(), log_type::omap, false);

  EXPECT_EQ(1, lg->got_entries.size());
  EXPECT_EQ(1, lg->got_entries[1].gen_id);
  EXPECT_EQ(log_type::omap, lg->got_entries[1].type);
  EXPECT_FALSE(lg->got_entries[1].pruned);

  lg->shutdown();
  lg.reset();

  lg = co_await logback_generations::init<generations>(
    dpp(), rados(), "foobar", pool(), log_type::fifo);

  EXPECT_EQ(2, lg->got_entries.size());
  EXPECT_EQ(0, lg->got_entries[0].gen_id);
  EXPECT_EQ(log_type::fifo, lg->got_entries[0].type);
  EXPECT_FALSE(lg->got_entries[0].pruned);

  EXPECT_EQ(1, lg->got_entries[1].gen_id);
  EXPECT_EQ(log_type::omap, lg->got_entries[1].type);
  EXPECT_FALSE(lg->got_entries[1].pruned);

  co_await lg->empty_to(dpp(), 0);

  EXPECT_EQ(0, *lg->tail);

  lg->shutdown();
  lg.reset();

  lg = co_await logback_generations::init<generations>(
    dpp(), rados(), "foobar", pool(), log_type::fifo);

  EXPECT_EQ(1, lg->got_entries.size());
  EXPECT_EQ(1, lg->got_entries[1].gen_id);
  EXPECT_EQ(log_type::omap, lg->got_entries[1].type);
  EXPECT_FALSE(lg->got_entries[1].pruned);
}

CORO_TEST_F(LogBacking, GenerationWN, NeoRadosTest) {
  // see above
  add_io_threads(2);
  auto lg1 = co_await logback_generations::init<generations>(
    dpp(), rados(), "foobar", pool(), log_type::fifo);

  co_await lg1->new_backing(dpp(), log_type::omap, false);

  EXPECT_EQ(1, lg1->got_entries.size());
  EXPECT_EQ(1, lg1->got_entries[1].gen_id);
  EXPECT_EQ(log_type::omap, lg1->got_entries[1].type);
  EXPECT_FALSE(lg1->got_entries[1].pruned);

  lg1->got_entries.clear();

  auto rados2 = co_await neorados::RADOS::Builder{}
    .build(asio_context, boost::asio::use_awaitable);

  auto lg2 = co_await logback_generations::init<generations>(
    dpp(), rados2, "foobar", pool(), log_type::fifo);

  EXPECT_EQ(2, lg2->got_entries.size());

  EXPECT_EQ(0, lg2->got_entries[0].gen_id);
  EXPECT_EQ(log_type::fifo, lg2->got_entries[0].type);
  EXPECT_FALSE(lg2->got_entries[0].pruned);

  EXPECT_EQ(1, lg2->got_entries[1].gen_id);
  EXPECT_EQ(log_type::omap, lg2->got_entries[1].type);
  EXPECT_FALSE(lg2->got_entries[1].pruned);

  lg2->got_entries.clear();

  co_await lg1->new_backing(dpp(), log_type::fifo, false);

  EXPECT_EQ(1, lg1->got_entries.size());
  EXPECT_EQ(2, lg1->got_entries[2].gen_id);
  EXPECT_EQ(log_type::fifo, lg1->got_entries[2].type);
  EXPECT_FALSE(lg1->got_entries[2].pruned);

  EXPECT_EQ(1, lg2->got_entries.size());
  EXPECT_EQ(2, lg2->got_entries[2].gen_id);
  EXPECT_EQ(log_type::fifo, lg2->got_entries[2].type);
  EXPECT_FALSE(lg2->got_entries[2].pruned);

  lg1->got_entries.clear();
  lg2->got_entries.clear();

  co_await lg2->empty_to(dpp(), 1);

  EXPECT_EQ(1, *lg1->tail);
  EXPECT_EQ(1, *lg2->tail);

  lg1->tail.reset();
  lg2->tail.reset();
}

// new_backing is a no-op when the head generation already has the same
// type and per_zonegroup flag.
CORO_TEST_F(LogBacking, NewBackingNoOp, NeoRadosTest) {
  add_io_threads(2);
  auto lg = co_await logback_generations::init<generations>(
    dpp(), rados(), "foobar_noop", pool(), log_type::omap);

  // Head is gen 0, omap, per_zonegroup=false.
  lg->got_entries.clear();

  // Calling new_backing with the same (omap, false) must not create a new gen.
  co_await lg->new_backing(dpp(), log_type::omap, false);
  EXPECT_TRUE(lg->got_entries.empty());

  lg->shutdown();
}

// new_backing opens a new generation when only per_zonegroup differs.
CORO_TEST_F(LogBacking, NewBackingPerZonegroupTransition, NeoRadosTest) {
  add_io_threads(2);
  auto lg = co_await logback_generations::init<generations>(
    dpp(), rados(), "foobar_pgzg", pool(), log_type::fifo);

  // Start: gen 0, fifo, per_zonegroup=false.
  EXPECT_FALSE(lg->got_entries[0].per_zonegroup);
  lg->got_entries.clear();

  // Flip to per_zonegroup=true while keeping fifo.
  co_await lg->new_backing(dpp(), log_type::fifo, true);

  EXPECT_EQ(1, lg->got_entries.size());
  EXPECT_TRUE(lg->got_entries.count(1));
  EXPECT_EQ(log_type::fifo, lg->got_entries[1].type);
  EXPECT_TRUE(lg->got_entries[1].per_zonegroup);

  lg->got_entries.clear();

  // Flip back to per_zonegroup=false — must open yet another generation.
  co_await lg->new_backing(dpp(), log_type::fifo, false);

  EXPECT_EQ(1, lg->got_entries.size());
  EXPECT_TRUE(lg->got_entries.count(2));
  EXPECT_EQ(log_type::fifo, lg->got_entries[2].type);
  EXPECT_FALSE(lg->got_entries[2].per_zonegroup);

  lg->shutdown();
}
