
#include <iostream>
#include <framework/index_factory.h>
#include <framework/index_helper.h>
#include <framework/index_memory.h>
#include <gtest/gtest.h>

using namespace zvec;
using namespace zvec::core;

TEST(MemoryStorage, General) {
  std::string file_path = "memory_storage_test_file";
  auto storage = IndexFactory::CreateStorage("MemoryStorage");
  ASSERT_TRUE(storage);
  EXPECT_NE(0, storage->open(file_path, false));

  ailego::Params params;
  EXPECT_EQ(0, storage->init(params));
  EXPECT_EQ(0, storage->open(file_path, true));

  IndexMeta meta;
  meta.set_trainer("trainer", 111, ailego::Params());
  meta.set_searcher("searcher", 222, ailego::Params());
  meta.set_builder("builder", 333, ailego::Params());

  EXPECT_EQ(0, IndexHelper::SerializeToStorage(meta, storage.get()));
  EXPECT_EQ(0, storage->flush());
  EXPECT_EQ(0, storage->close());

  // Reopen it
  EXPECT_EQ(0, storage->open(file_path, false));

  IndexMeta meta2;
  EXPECT_EQ(0, IndexHelper::DeserializeFromStorage(storage.get(), &meta2));
  EXPECT_EQ(0, storage->flush());

  EXPECT_EQ(0, storage->append("AAAA", 1234));
  EXPECT_EQ(0, storage->append("BBBB", 1234));

  {
    auto storage1 = IndexFactory::CreateStorage("MemoryStorage");
    ASSERT_TRUE(storage1);
    EXPECT_EQ(0, storage1->open(file_path, false));
  }

  auto aaaa = storage->get("AAAA");
  ASSERT_TRUE(aaaa);
  std::string hello = "Hello world!!!";
  EXPECT_EQ(hello.size(), aaaa->write(0, hello.data(), hello.size()));
  const void *data;
  EXPECT_EQ(hello.size(), aaaa->read(0, &data, hello.size()));
  EXPECT_EQ(hello, std::string((const char *)data, hello.size()));

  EXPECT_EQ(0u, storage->check_point());
  storage->refresh(1234);
  EXPECT_EQ(1234u, storage->check_point());

  {
    auto storage1 = IndexFactory::CreateStorage("MemoryStorage");
    ASSERT_TRUE(storage1);
    EXPECT_EQ(0, storage1->open(file_path, false));
  }

  IndexMemory::Instance()->remove(file_path);
  {
    auto storage1 = IndexFactory::CreateStorage("MemoryStorage");
    ASSERT_TRUE(storage1);
    EXPECT_EQ(IndexError_NoExist, storage1->open(file_path, false));
  }

  {
    const char *temp_path = "temp_path";
    auto storage1 = IndexFactory::CreateStorage("MemoryStorage");
    ASSERT_TRUE(storage1);
    EXPECT_EQ(0, storage1->open(temp_path, true));
    storage1 = nullptr;
    EXPECT_TRUE(IndexMemory::Instance()->has(temp_path));
    IndexMemory::Instance()->remove(temp_path);
    EXPECT_FALSE(IndexMemory::Instance()->has(temp_path));

    auto storage2 = IndexFactory::CreateStorage("TemporaryMemoryStorage");
    ASSERT_TRUE(storage2);
    EXPECT_EQ(0, storage2->open(temp_path, true));
    EXPECT_TRUE(IndexMemory::Instance()->has(temp_path));
    storage2 = nullptr;
    EXPECT_FALSE(IndexMemory::Instance()->has(temp_path));
  }
}