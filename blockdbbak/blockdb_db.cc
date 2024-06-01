//
//  leveldb_db.cc
//  YCSB-cpp
//
//  Copyright (c) 2020 Youngjae Lee <ls4154.lee@gmail.com>.
//  Modifications Copyright 2023 Chengye YU <yuchengye2013 AT outlook.com>.
//

#include "blockdb_db.h"
#include "core/core_workload.h"
#include "core/db_factory.h"
#include "utils/utils.h"


namespace {
  const std::string PROP_NAME = "dbpath";
  const std::string PROP_NAME_DEFAULT = "";

  const std::string PROP_FORMAT = "leveldb.format";
  const std::string PROP_FORMAT_DEFAULT = "single";

  const std::string PROP_DESTROY = "leveldb.destroy";
  const std::string PROP_DESTROY_DEFAULT = "false";

  const std::string PROP_COMPRESSION = "leveldb.compression";
  const std::string PROP_COMPRESSION_DEFAULT = "no";

  const std::string PROP_WRITE_BUFFER_SIZE = "leveldb.write_buffer_size";
  const std::string PROP_WRITE_BUFFER_SIZE_DEFAULT = "0";

  const std::string PROP_MAX_FILE_SIZE = "leveldb.max_file_size";
  const std::string PROP_MAX_FILE_SIZE_DEFAULT = "0";

  const std::string PROP_MAX_OPEN_FILES = "leveldb.max_open_files";
  const std::string PROP_MAX_OPEN_FILES_DEFAULT = "0";

  const std::string PROP_CACHE_SIZE = "leveldb.cache_size";
  const std::string PROP_CACHE_SIZE_DEFAULT = "0";

  const std::string PROP_FILTER_BITS = "leveldb.filter_bits";
  const std::string PROP_FILTER_BITS_DEFAULT = "0";

  const std::string PROP_BLOCK_SIZE = "leveldb.block_size";
  const std::string PROP_BLOCK_SIZE_DEFAULT = "0";

  const std::string PROP_BLOCK_RESTART_INTERVAL = "leveldb.block_restart_interval";
  const std::string PROP_BLOCK_RESTART_INTERVAL_DEFAULT = "0";

  const std::string PROP_DIRECT_IO = "leveldb.direct_io";
  const std::string PROP_DIRECT_IO_DEFAULT = "0";
  
  const std::string PROP_CREATE_IF_MISSING = "leveldb.create_if_missing";
  const std::string PROP_CREATE_IF_MISSING_DEFAULT = "0";

  const std::string PROP_COMPACTION = "leveldb.compaction";
  const std::string PROP_COMPACTION_DEFAULT = "no";

  const std::string PROP_LEVEL0_SLOWDOWN_WRITES_TRIGGER = "leveldb.level0_slowdown_writes_trigger";
  const std::string PROP_LEVEL0_SLOWDOWN_WRITES_TRIGGER_DEFAULT = "0";

  const std::string PROP_LEVEL0_STOP_WRITES_TRIGGER = "leveldb.level0_stop_writes_trigger";
  const std::string PROP_LEVEL0_STOP_WRITES_TRIGGER_DEFAULT = "0";

  const std::string PROP_SYNC = "leveldb.sync";
  const std::string PROP_SYNC_DEFAULT = "0";

  const std::string PROP_LAST_LEVEL = "leveldb.last_level";
  const std::string PROP_LAST_LEVEL_DEFAULT = "0";
  
} // anonymous

namespace ycsbc {

leveldb::DB *LeveldbDB::db_ = nullptr;
int LeveldbDB::ref_cnt_ = 0;
std::mutex LeveldbDB::mu_;

void LeveldbDB::Init() {
  const std::lock_guard<std::mutex> lock(mu_);

  const utils::Properties &props = *props_;
  const std::string &format = props.GetProperty(PROP_FORMAT, PROP_FORMAT_DEFAULT);
  if (format == "single") {
    format_ = kSingleEntry;
    method_read_ = &LeveldbDB::ReadSingleEntry;
    method_scan_ = &LeveldbDB::ScanSingleEntry;
    method_update_ = &LeveldbDB::UpdateSingleEntry;
    method_insert_ = &LeveldbDB::InsertSingleEntry;
    method_delete_ = &LeveldbDB::DeleteSingleEntry;
  } else if (format == "row") {
    format_ = kRowMajor;
    method_read_ = &LeveldbDB::ReadCompKeyRM;
    method_scan_ = &LeveldbDB::ScanCompKeyRM;
    method_update_ = &LeveldbDB::InsertCompKey;
    method_insert_ = &LeveldbDB::InsertCompKey;
    method_delete_ = &LeveldbDB::DeleteCompKey;
  } else if (format == "column") {
    format_ = kColumnMajor;
    method_read_ = &LeveldbDB::ReadCompKeyCM;
    method_scan_ = &LeveldbDB::ScanCompKeyCM;
    method_update_ = &LeveldbDB::InsertCompKey;
    method_insert_ = &LeveldbDB::InsertCompKey;
    method_delete_ = &LeveldbDB::DeleteCompKey;
  } else {
    throw utils::Exception("unknown format");
  }
  fieldcount_ = std::stoi(props.GetProperty(CoreWorkload::FIELD_COUNT_PROPERTY,
                                            CoreWorkload::FIELD_COUNT_DEFAULT));
  field_prefix_ = props.GetProperty(CoreWorkload::FIELD_NAME_PREFIX,
                                    CoreWorkload::FIELD_NAME_PREFIX_DEFAULT);

  ref_cnt_++;
  if (db_) {
    return;
  }

  const std::string &db_path = props.GetProperty(PROP_NAME, PROP_NAME_DEFAULT);
  if (db_path == "") {
    throw utils::Exception("LevelDB db path is missing");
  }
  else
  {
    std::cout << "LevelDB db path: " << db_path << std::endl;
  }

  leveldb::Options opt;
  GetOptions(props, &opt);

  // 打印设置的值
  std::cout << "Create if missing: " << (opt.create_if_missing ? "true" : "false") << std::endl;
  std::cout << "Compression: " << (opt.compression == leveldb::kNoCompression ? "No" : "Yes") << std::endl;
  std::cout << "Write buffer size: " << opt.write_buffer_size << std::endl;
  std::cout << "Compaction style: " << (opt.compaction == leveldb::kBlockCompaction ? "Block" : "table")<< std::endl;
  std::cout << "Bloom filter bits per key: " << ((opt.filter_policy != nullptr) ? "opt.filter_policy exisit" : "None") << std::endl;
  std::cout << "Level0 slowdown writes trigger: " << opt.level0_slowdown_writes_trigger << std::endl;
  std::cout << "Level0 stop writes trigger: " << opt.level0_stop_writes_trigger << std::endl;
  std::cout << "Block cache size: " << ((opt.block_cache != nullptr) ? "opt.block_cache exisit" : "None") << std::endl;
  std::cout << "Direct I/O: " << (opt.direct_io ? "true" : "false") << std::endl;
  std::cout << "Max file size: " << opt.max_file_size << std::endl;
  
  // opt.create_if_missing = true;
  // opt.compression = no;
  // opt.write_buffer_size = (16 << 20);
  // opt.compaction = leveldb::kBlockCompaction;
  // opt.filter_policy = leveldb::NewBloomFilterPolicy(10);
  // opt.level0_slowdown_writes_trigger = 12;
  // opt.level0_stop_writes_trigger = 16;
  // opt.block_cache = leveldb::NewLRUCache(4LL << 30);
  //opt.direct_io = true;
  // opt.max_file_size = (16 << 20);

  leveldb::Status s;

  if (props.GetProperty(PROP_DESTROY, PROP_DESTROY_DEFAULT) == "true") {
    s = leveldb::DestroyDB(db_path, opt);
    if (!s.ok()) {
      throw utils::Exception(std::string("LevelDB DestroyDB: ") + s.ToString());
    }
  }
  if(nullptr == db_)
  {
    s = leveldb::DB::Open(opt, db_path, &db_);
  }

  if (!s.ok()) {
    throw utils::Exception(std::string("LevelDB Open: ") + s.ToString());
  }
}

void LeveldbDB::Cleanup() {
  const std::lock_guard<std::mutex> lock(mu_);
  if (--ref_cnt_) {
    return;
  }
  // std::cout << "171 delete db_" << std::endl;
  // delete db_;
  // db_ = nullptr;
}

void LeveldbDB::GetOptions(const utils::Properties &props, leveldb::Options *opt) {
  long long writer_buffer_size = std::stoll(props.GetProperty(PROP_WRITE_BUFFER_SIZE,
                                                          PROP_WRITE_BUFFER_SIZE_DEFAULT));
  if (writer_buffer_size > 0) {
    opt->write_buffer_size = writer_buffer_size;
  }
  long long max_file_size = std::stoll(props.GetProperty(PROP_MAX_FILE_SIZE,
                                                     PROP_MAX_FILE_SIZE_DEFAULT));
  if (max_file_size > 0) {
    opt->max_file_size = max_file_size;
  }
  long long cache_size = std::stoll(props.GetProperty(PROP_CACHE_SIZE,
                                                  PROP_CACHE_SIZE_DEFAULT));
  if (cache_size > 0) {
    opt->block_cache = leveldb::NewLRUCache(cache_size);
  }
  int max_open_files = std::stoi(props.GetProperty(PROP_MAX_OPEN_FILES,
                                                   PROP_MAX_OPEN_FILES_DEFAULT));
  if (max_open_files > 0) {
    opt->max_open_files = max_open_files;
  }
  std::string compression = props.GetProperty(PROP_COMPRESSION,
                                              PROP_COMPRESSION_DEFAULT);
  if (compression == "snappy") {
    opt->compression = leveldb::kSnappyCompression;
  } else {
    opt->compression = leveldb::kNoCompression;
  }
  int filter_bits = std::stoi(props.GetProperty(PROP_FILTER_BITS,
                                                PROP_FILTER_BITS_DEFAULT));
  if (filter_bits > 0) {
    opt->filter_policy = leveldb::NewBloomFilterPolicy(filter_bits);
  }
  int block_size = std::stoi(props.GetProperty(PROP_BLOCK_SIZE,
                                               PROP_BLOCK_SIZE_DEFAULT)); 
  if (block_size > 0) {
    opt->block_size = block_size;
  }
  int block_restart_interval = std::stoi(props.GetProperty(PROP_BLOCK_RESTART_INTERVAL,
                                                PROP_BLOCK_RESTART_INTERVAL_DEFAULT));
  if (block_restart_interval > 0) {
    opt->block_restart_interval = block_restart_interval;
  }

  std::string compaction = props.GetProperty(PROP_COMPACTION,
                                              PROP_COMPACTION_DEFAULT);
  if (compaction == "block") {
    opt->compaction = leveldb::kBlockCompaction;
  } else {
    opt->compaction = leveldb::kTableCompaction;
  }

  int level0_slowdown_writes_trigger = std::stoi(props.GetProperty(PROP_LEVEL0_SLOWDOWN_WRITES_TRIGGER,
                                               PROP_LEVEL0_SLOWDOWN_WRITES_TRIGGER_DEFAULT)); 
  if(level0_slowdown_writes_trigger > 0)
  {
    opt->level0_slowdown_writes_trigger = level0_slowdown_writes_trigger;
  }

  int level0_stop_writes_trigger = std::stoi(props.GetProperty(PROP_LEVEL0_STOP_WRITES_TRIGGER,
                                               PROP_LEVEL0_STOP_WRITES_TRIGGER_DEFAULT)); 
  if(level0_stop_writes_trigger > 0)
  {
    opt->level0_stop_writes_trigger = level0_stop_writes_trigger;
  }

  int direct_io = std::stoi(props.GetProperty(PROP_DIRECT_IO,
                                               PROP_DIRECT_IO_DEFAULT)); 
  if(direct_io > 0)
  {
    opt->direct_io = direct_io;
  }

  int create_if_missing = std::stoi(props.GetProperty(PROP_CREATE_IF_MISSING,
                                               PROP_CREATE_IF_MISSING_DEFAULT)); 
  if(create_if_missing > 0)
  {
    opt->create_if_missing = create_if_missing;
  }

  int last_level = std::stoi(props.GetProperty(PROP_LAST_LEVEL,
                                               PROP_LAST_LEVEL_DEFAULT)); 
  if(last_level > 0)
  {
    opt->last_level = last_level;
  }

}

void LeveldbDB::GetWriteOptions(const utils::Properties &props, leveldb::WriteOptions *write_opt)
{  
  int sync = std::stoi(props.GetProperty(PROP_SYNC,
                                               PROP_SYNC_DEFAULT)); 
  if(sync > 0)
  {
    write_opt->sync = sync;
  }
}

void LeveldbDB::SerializeRow(const std::vector<Field> &values, std::string *data) {
  for (const Field &field : values) {
    uint32_t len = field.name.size();
    data->append(reinterpret_cast<char *>(&len), sizeof(uint32_t));
    data->append(field.name.data(), field.name.size());
    len = field.value.size();
    data->append(reinterpret_cast<char *>(&len), sizeof(uint32_t));
    data->append(field.value.data(), field.value.size());
  }
}

void LeveldbDB::DeserializeRowFilter(std::vector<Field> *values, const std::string &data,
                                     const std::vector<std::string> &fields) {
  const char *p = data.data();
  const char *lim = p + data.size();

  std::vector<std::string>::const_iterator filter_iter = fields.begin();
  while (p != lim && filter_iter != fields.end()) {
    assert(p < lim);
    uint32_t len = *reinterpret_cast<const uint32_t *>(p);
    p += sizeof(uint32_t);
    std::string field(p, static_cast<const size_t>(len));
    p += len;
    len = *reinterpret_cast<const uint32_t *>(p);
    p += sizeof(uint32_t);
    std::string value(p, static_cast<const size_t>(len));
    p += len;
    if (*filter_iter == field) {
      values->push_back({field, value});
      filter_iter++;
    }
  }
  assert(values->size() == fields.size());
}

void LeveldbDB::DeserializeRow(std::vector<Field> *values, const std::string &data) {
  const char *p = data.data();
  const char *lim = p + data.size();
  while (p != lim) {
    assert(p < lim);
    uint32_t len = *reinterpret_cast<const uint32_t *>(p);
    p += sizeof(uint32_t);
    std::string field(p, static_cast<const size_t>(len));
    p += len;
    len = *reinterpret_cast<const uint32_t *>(p);
    p += sizeof(uint32_t);
    std::string value(p, static_cast<const size_t>(len));
    p += len;
    values->push_back({field, value});
  }
  assert(values->size() == fieldcount_);
}

std::string LeveldbDB::BuildCompKey(const std::string &key, const std::string &field_name) {
  switch (format_) {
    case kRowMajor:
      return key + ":" + field_name;
      break;
    case kColumnMajor:
      return field_name + ":" + key;
      break;
    default:
      throw utils::Exception("wrong format");
  }
}

std::string LeveldbDB::KeyFromCompKey(const std::string &comp_key) {
  size_t idx = comp_key.find(":");
  assert(idx != std::string::npos);
  return comp_key.substr(0, idx);
}

std::string LeveldbDB::FieldFromCompKey(const std::string &comp_key) {
  size_t idx = comp_key.find(":");
  assert(idx != std::string::npos);
  return comp_key.substr(idx + 1);
}

DB::Status LeveldbDB::ReadSingleEntry(const std::string &table, const std::string &key,
                                      const std::vector<std::string> *fields,
                                      std::vector<Field> &result) {
  std::string data;
  leveldb::Status s = db_->Get(leveldb::ReadOptions(), key, &data);
  if (s.IsNotFound()) {
    std::cout << "zc LeveldbDB::ReadSingleEntry::db_->Get not found" << std::endl;
    return kNotFound;
  } else if (!s.ok()) {
    throw utils::Exception(std::string("LevelDB Get: ") + s.ToString());
  }

  if (fields != nullptr) {
    DeserializeRowFilter(&result, data, *fields);
  } else {
    DeserializeRow(&result, data);
  }
  return kOK;
}

DB::Status LeveldbDB::ScanSingleEntry(const std::string &table, const std::string &key, int len,
                                      const std::vector<std::string> *fields,
                                      std::vector<std::vector<Field>> &result) {
  leveldb::Iterator *db_iter = db_->NewIterator(leveldb::ReadOptions());
  db_iter->Seek(key);
  for (int i = 0; db_iter->Valid() && i < len; i++) {
    std::string data = db_iter->value().ToString();
    result.push_back(std::vector<Field>());
    std::vector<Field> &values = result.back();
    if (fields != nullptr) {
      DeserializeRowFilter(&values, data, *fields);
    } else {
      DeserializeRow(&values, data);
    }
    db_iter->Next();
  }
  delete db_iter;
  return kOK;
}

DB::Status LeveldbDB::UpdateSingleEntry(const std::string &table, const std::string &key,
                                        std::vector<Field> &values) {
  std::string data;
  leveldb::Status s = db_->Get(leveldb::ReadOptions(), key, &data);
  if (s.IsNotFound()) {
    std::cout << "zc LeveldbDB::UpdateSingleEntry::db_->Get not found" << std::endl;
    return kNotFound;
  } else if (!s.ok()) {
    throw utils::Exception(std::string("LevelDB Get: ") + s.ToString());
  }
  std::cout << "zc LeveldbDB::UpdateSingleEntry::db_->Get found" << std::endl;
  std::vector<Field> current_values;
  DeserializeRow(&current_values, data);
  for (Field &new_field : values) {
    bool found MAYBE_UNUSED = false;
    for (Field &cur_field : current_values) {
      if (cur_field.name == new_field.name) {
        found = true;
        cur_field.value = new_field.value;
        break;
      }
    }
    assert(found);
  }
  leveldb::WriteOptions wopt;

  data.clear();
  SerializeRow(current_values, &data);
  s = db_->Put(wopt, key, data);
  if (!s.ok()) {
    throw utils::Exception(std::string("LevelDB Put: ") + s.ToString());
  }
  return kOK;
}

DB::Status LeveldbDB::InsertSingleEntry(const std::string &table, const std::string &key,
                                        std::vector<Field> &values) {
  std::string data;
  SerializeRow(values, &data);
  leveldb::WriteOptions wopt;
  const utils::Properties &props = *props_;
  GetWriteOptions(props, &wopt);

  leveldb::Status s = db_->Put(wopt, key, data);
  if (!s.ok()) {
    throw utils::Exception(std::string("LevelDB Put: ") + s.ToString());
  }
  return kOK;
}

DB::Status LeveldbDB::DeleteSingleEntry(const std::string &table, const std::string &key) {
  leveldb::WriteOptions wopt;
  leveldb::Status s = db_->Delete(wopt, key);
  if (!s.ok()) {
    throw utils::Exception(std::string("LevelDB Delete: ") + s.ToString());
  }
  return kOK;
}

DB::Status LeveldbDB::ReadCompKeyRM(const std::string &table, const std::string &key,
                                    const std::vector<std::string> *fields,
                                    std::vector<Field> &result) {
  leveldb::Iterator *db_iter = db_->NewIterator(leveldb::ReadOptions());
  db_iter->Seek(key);
  if (!db_iter->Valid() || KeyFromCompKey(db_iter->key().ToString()) != key) {
    return kNotFound;
  }
  if (fields != nullptr) {
    std::vector<std::string>::const_iterator filter_iter = fields->begin();
    for (int i = 0; i < fieldcount_ && filter_iter != fields->end() && db_iter->Valid(); i++) {
      std::string comp_key = db_iter->key().ToString();
      std::string cur_val = db_iter->value().ToString();
      std::string cur_key = KeyFromCompKey(comp_key);
      std::string cur_field = FieldFromCompKey(comp_key);
      assert(cur_key == key);
      assert(cur_field == field_prefix_ + std::to_string(i));

      if (cur_field == *filter_iter) {
        result.push_back({cur_field, cur_val});
        filter_iter++;
      }
      db_iter->Next();
    }
    assert(result.size() == fields->size());
  } else {
    for (int i = 0; i < fieldcount_ && db_iter->Valid(); i++) {
      std::string comp_key = db_iter->key().ToString();
      std::string cur_val = db_iter->value().ToString();
      std::string cur_key = KeyFromCompKey(comp_key);
      std::string cur_field = FieldFromCompKey(comp_key);
      assert(cur_key == key);
      assert(cur_field == field_prefix_ + std::to_string(i));

      result.push_back({cur_field, cur_val});
      db_iter->Next();
    }
    assert(result.size() == fieldcount_);
  }
  delete db_iter;
  return kOK;
}

DB::Status LeveldbDB::ScanCompKeyRM(const std::string &table, const std::string &key, int len,
                                    const std::vector<std::string> *fields,
                                    std::vector<std::vector<Field>> &result) {
  leveldb::Iterator *db_iter = db_->NewIterator(leveldb::ReadOptions());
  db_iter->Seek(key);
  assert(db_iter->Valid() && KeyFromCompKey(db_iter->key().ToString()) == key);
  for (int i = 0; i < len && db_iter->Valid(); i++) {
    result.push_back(std::vector<Field>());
    std::vector<Field> &values = result.back();
    if (fields != nullptr) {
      std::vector<std::string>::const_iterator filter_iter = fields->begin();
      for (int j = 0; j < fieldcount_ && filter_iter != fields->end() && db_iter->Valid(); j++) {
        std::string comp_key = db_iter->key().ToString();
        std::string cur_val = db_iter->value().ToString();
        std::string cur_key = KeyFromCompKey(comp_key);
        std::string cur_field = FieldFromCompKey(comp_key);
        assert(cur_field == field_prefix_ + std::to_string(j));

        if (cur_field == *filter_iter) {
          values.push_back({cur_field, cur_val});
          filter_iter++;
        }
        db_iter->Next();
      }
      assert(values.size() == fields->size());
    } else {
      for (int j = 0; j < fieldcount_ && db_iter->Valid(); j++) {
        std::string comp_key = db_iter->key().ToString();
        std::string cur_val = db_iter->value().ToString();
        std::string cur_key = KeyFromCompKey(comp_key);
        std::string cur_field = FieldFromCompKey(comp_key);
        assert(cur_field == field_prefix_ + std::to_string(j));

        values.push_back({cur_field, cur_val});
        db_iter->Next();
      }
      assert(values.size() == fieldcount_);
    }
  }
  delete db_iter;
  return kOK;
}

DB::Status LeveldbDB::ReadCompKeyCM(const std::string &table, const std::string &key,
                                    const std::vector<std::string> *fields,
                                    std::vector<Field> &result) {
  return kNotImplemented;
}

DB::Status LeveldbDB::ScanCompKeyCM(const std::string &table, const std::string &key, int len,
                                    const std::vector<std::string> *fields,
                                    std::vector<std::vector<Field>> &result) {
  return kNotImplemented;
}

DB::Status LeveldbDB::InsertCompKey(const std::string &table, const std::string &key,
                                    std::vector<Field> &values) {
  leveldb::WriteOptions wopt;
  leveldb::WriteBatch batch;

  std::string comp_key;
  for (Field &field : values) {
    comp_key = BuildCompKey(key, field.name);
    batch.Put(comp_key, field.value);
  }

  leveldb::Status s = db_->Write(wopt, &batch);
  if (!s.ok()) {
    throw utils::Exception(std::string("LevelDB Write: ") + s.ToString());
  }
  return kOK;
}

DB::Status LeveldbDB::DeleteCompKey(const std::string &table, const std::string &key) {
  leveldb::WriteOptions wopt;
  leveldb::WriteBatch batch;

  std::string comp_key;
  for (int i = 0; i < fieldcount_; i++) {
    comp_key = BuildCompKey(key, field_prefix_ + std::to_string(i));
    batch.Delete(comp_key);
  }

  leveldb::Status s = db_->Write(wopt, &batch);
  if (!s.ok()) {
    throw utils::Exception(std::string("LevelDB Write: ") + s.ToString());
  }
  return kOK;
}

void LeveldbDB::PrintStats() { 
    std::string stats;
    if(nullptr == db_)
    {
      throw utils::Exception("LeveldbDB::PrintStats failed: db has been deleted.");
      return;
    }
    if(db_->GetProperty("leveldb.stats",&stats) == true)
    {
      std::cout << stats << std::endl;
    }
    else
    {
      throw utils::Exception("LeveldbDB::PrintStats failed: there is an issue with the native interface.");
    }

}

DB *NewLeveldbDB() {
  return new LeveldbDB;
}

const bool registered = DBFactory::RegisterDB("blockdb", NewLeveldbDB);

} // ycsbc
