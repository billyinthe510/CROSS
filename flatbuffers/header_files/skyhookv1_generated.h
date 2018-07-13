// automatically generated by the FlatBuffers compiler, do not modify


#ifndef FLATBUFFERS_GENERATED_SKYHOOKV1_TABLES_H_
#define FLATBUFFERS_GENERATED_SKYHOOKV1_TABLES_H_

#include "flatbuffers/flatbuffers.h"
#include "flatbuffers/flexbuffers.h"

namespace Tables {

struct Table;

struct Row;

struct Table FLATBUFFERS_FINAL_CLASS : private flatbuffers::Table {
  enum {
    VT_SKYHOOK_VERSION = 4,
    VT_SCHEMA_VERSION = 6,
    VT_TABLE_NAME = 8,
    VT_DELETE_VECTOR = 10,
    VT_ROWS = 12,
    VT_NROWS = 14
  };
  uint8_t skyhook_version() const {
    return GetField<uint8_t>(VT_SKYHOOK_VERSION, 0);
  }
  uint8_t schema_version() const {
    return GetField<uint8_t>(VT_SCHEMA_VERSION, 0);
  }
  const flatbuffers::String *table_name() const {
    return GetPointer<const flatbuffers::String *>(VT_TABLE_NAME);
  }
  const flatbuffers::Vector<uint8_t> *delete_vector() const {
    return GetPointer<const flatbuffers::Vector<uint8_t> *>(VT_DELETE_VECTOR);
  }
  const flatbuffers::Vector<flatbuffers::Offset<Row>> *rows() const {
    return GetPointer<const flatbuffers::Vector<flatbuffers::Offset<Row>> *>(VT_ROWS);
  }
  uint32_t nrows() const {
    return GetField<uint32_t>(VT_NROWS, 0);
  }
  bool Verify(flatbuffers::Verifier &verifier) const {
    return VerifyTableStart(verifier) &&
           VerifyField<uint8_t>(verifier, VT_SKYHOOK_VERSION) &&
           VerifyField<uint8_t>(verifier, VT_SCHEMA_VERSION) &&
           VerifyOffset(verifier, VT_TABLE_NAME) &&
           verifier.Verify(table_name()) &&
           VerifyOffset(verifier, VT_DELETE_VECTOR) &&
           verifier.Verify(delete_vector()) &&
           VerifyOffset(verifier, VT_ROWS) &&
           verifier.Verify(rows()) &&
           verifier.VerifyVectorOfTables(rows()) &&
           VerifyField<uint32_t>(verifier, VT_NROWS) &&
           verifier.EndTable();
  }
};

struct TableBuilder {
  flatbuffers::FlatBufferBuilder &fbb_;
  flatbuffers::uoffset_t start_;
  void add_skyhook_version(uint8_t skyhook_version) {
    fbb_.AddElement<uint8_t>(Table::VT_SKYHOOK_VERSION, skyhook_version, 0);
  }
  void add_schema_version(uint8_t schema_version) {
    fbb_.AddElement<uint8_t>(Table::VT_SCHEMA_VERSION, schema_version, 0);
  }
  void add_table_name(flatbuffers::Offset<flatbuffers::String> table_name) {
    fbb_.AddOffset(Table::VT_TABLE_NAME, table_name);
  }
  void add_delete_vector(flatbuffers::Offset<flatbuffers::Vector<uint8_t>> delete_vector) {
    fbb_.AddOffset(Table::VT_DELETE_VECTOR, delete_vector);
  }
  void add_rows(flatbuffers::Offset<flatbuffers::Vector<flatbuffers::Offset<Row>>> rows) {
    fbb_.AddOffset(Table::VT_ROWS, rows);
  }
  void add_nrows(uint32_t nrows) {
    fbb_.AddElement<uint32_t>(Table::VT_NROWS, nrows, 0);
  }
  explicit TableBuilder(flatbuffers::FlatBufferBuilder &_fbb)
        : fbb_(_fbb) {
    start_ = fbb_.StartTable();
  }
  TableBuilder &operator=(const TableBuilder &);
  flatbuffers::Offset<Table> Finish() {
    const auto end = fbb_.EndTable(start_);
    auto o = flatbuffers::Offset<Table>(end);
    return o;
  }
};

inline flatbuffers::Offset<Table> CreateTable(
    flatbuffers::FlatBufferBuilder &_fbb,
    uint8_t skyhook_version = 0,
    uint8_t schema_version = 0,
    flatbuffers::Offset<flatbuffers::String> table_name = 0,
    flatbuffers::Offset<flatbuffers::Vector<uint8_t>> delete_vector = 0,
    flatbuffers::Offset<flatbuffers::Vector<flatbuffers::Offset<Row>>> rows = 0,
    uint32_t nrows = 0) {
  TableBuilder builder_(_fbb);
  builder_.add_nrows(nrows);
  builder_.add_rows(rows);
  builder_.add_delete_vector(delete_vector);
  builder_.add_table_name(table_name);
  builder_.add_schema_version(schema_version);
  builder_.add_skyhook_version(skyhook_version);
  return builder_.Finish();
}

inline flatbuffers::Offset<Table> CreateTableDirect(
    flatbuffers::FlatBufferBuilder &_fbb,
    uint8_t skyhook_version = 0,
    uint8_t schema_version = 0,
    const char *table_name = nullptr,
    const std::vector<uint8_t> *delete_vector = nullptr,
    const std::vector<flatbuffers::Offset<Row>> *rows = nullptr,
    uint32_t nrows = 0) {
  return Tables::CreateTable(
      _fbb,
      skyhook_version,
      schema_version,
      table_name ? _fbb.CreateString(table_name) : 0,
      delete_vector ? _fbb.CreateVector<uint8_t>(*delete_vector) : 0,
      rows ? _fbb.CreateVector<flatbuffers::Offset<Row>>(*rows) : 0,
      nrows);
}

struct Row FLATBUFFERS_FINAL_CLASS : private flatbuffers::Table {
  enum {
    VT_RID = 4,
    VT_NULLBITS = 6,
    VT_DATA = 8
  };
  uint64_t RID() const {
    return GetField<uint64_t>(VT_RID, 0);
  }
  const flatbuffers::Vector<uint64_t> *nullbits() const {
    return GetPointer<const flatbuffers::Vector<uint64_t> *>(VT_NULLBITS);
  }
  const flatbuffers::Vector<uint8_t> *data() const {
    return GetPointer<const flatbuffers::Vector<uint8_t> *>(VT_DATA);
  }
  flexbuffers::Reference data_flexbuffer_root() const {
    auto v = data();
    return flexbuffers::GetRoot(v->Data(), v->size());
  }
  bool Verify(flatbuffers::Verifier &verifier) const {
    return VerifyTableStart(verifier) &&
           VerifyField<uint64_t>(verifier, VT_RID) &&
           VerifyOffset(verifier, VT_NULLBITS) &&
           verifier.Verify(nullbits()) &&
           VerifyOffset(verifier, VT_DATA) &&
           verifier.Verify(data()) &&
           verifier.EndTable();
  }
};

struct RowBuilder {
  flatbuffers::FlatBufferBuilder &fbb_;
  flatbuffers::uoffset_t start_;
  void add_RID(uint64_t RID) {
    fbb_.AddElement<uint64_t>(Row::VT_RID, RID, 0);
  }
  void add_nullbits(flatbuffers::Offset<flatbuffers::Vector<uint64_t>> nullbits) {
    fbb_.AddOffset(Row::VT_NULLBITS, nullbits);
  }
  void add_data(flatbuffers::Offset<flatbuffers::Vector<uint8_t>> data) {
    fbb_.AddOffset(Row::VT_DATA, data);
  }
  explicit RowBuilder(flatbuffers::FlatBufferBuilder &_fbb)
        : fbb_(_fbb) {
    start_ = fbb_.StartTable();
  }
  RowBuilder &operator=(const RowBuilder &);
  flatbuffers::Offset<Row> Finish() {
    const auto end = fbb_.EndTable(start_);
    auto o = flatbuffers::Offset<Row>(end);
    return o;
  }
};

inline flatbuffers::Offset<Row> CreateRow(
    flatbuffers::FlatBufferBuilder &_fbb,
    uint64_t RID = 0,
    flatbuffers::Offset<flatbuffers::Vector<uint64_t>> nullbits = 0,
    flatbuffers::Offset<flatbuffers::Vector<uint8_t>> data = 0) {
  RowBuilder builder_(_fbb);
  builder_.add_RID(RID);
  builder_.add_data(data);
  builder_.add_nullbits(nullbits);
  return builder_.Finish();
}

inline flatbuffers::Offset<Row> CreateRowDirect(
    flatbuffers::FlatBufferBuilder &_fbb,
    uint64_t RID = 0,
    const std::vector<uint64_t> *nullbits = nullptr,
    const std::vector<uint8_t> *data = nullptr) {
  return Tables::CreateRow(
      _fbb,
      RID,
      nullbits ? _fbb.CreateVector<uint64_t>(*nullbits) : 0,
      data ? _fbb.CreateVector<uint8_t>(*data) : 0);
}

inline const Tables::Table *GetTable(const void *buf) {
  return flatbuffers::GetRoot<Tables::Table>(buf);
}

inline const Tables::Table *GetSizePrefixedTable(const void *buf) {
  return flatbuffers::GetSizePrefixedRoot<Tables::Table>(buf);
}

inline bool VerifyTableBuffer(
    flatbuffers::Verifier &verifier) {
  return verifier.VerifyBuffer<Tables::Table>(nullptr);
}

inline bool VerifySizePrefixedTableBuffer(
    flatbuffers::Verifier &verifier) {
  return verifier.VerifySizePrefixedBuffer<Tables::Table>(nullptr);
}

inline void FinishTableBuffer(
    flatbuffers::FlatBufferBuilder &fbb,
    flatbuffers::Offset<Tables::Table> root) {
  fbb.Finish(root);
}

inline void FinishSizePrefixedTableBuffer(
    flatbuffers::FlatBufferBuilder &fbb,
    flatbuffers::Offset<Tables::Table> root) {
  fbb.FinishSizePrefixed(root);
}

}  // namespace Tables

#endif  // FLATBUFFERS_GENERATED_SKYHOOKV1_TABLES_H_
