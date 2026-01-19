#include <cstdlib>
#include <zvec/db/collection.h>
#include <zvec/db/status.h>
#include "zvec/db/doc.h"
#include "zvec/db/schema.h"

using namespace zvec;

CollectionSchema::Ptr create_schema() {
  auto schema = std::make_shared<CollectionSchema>("demo");
  schema->set_max_doc_count_per_segment(1000);

  schema->add_field(std::make_shared<FieldSchema>(
      "id", DataType::INT64, false, std::make_shared<InvertIndexParams>(true)));
  schema->add_field(std::make_shared<FieldSchema>(
      "name", DataType::STRING, false,
      std::make_shared<InvertIndexParams>(false)));
  schema->add_field(
      std::make_shared<FieldSchema>("weight", DataType::FLOAT, true));

  schema->add_field(std::make_shared<FieldSchema>(
      "dense", DataType::VECTOR_FP32, 128, false,
      std::make_shared<HnswIndexParams>(MetricType::IP)));
  schema->add_field(std::make_shared<FieldSchema>(
      "sparse", DataType::SPARSE_VECTOR_FP32, 0, false,
      std::make_shared<HnswIndexParams>(MetricType::IP)));

  return schema;
}

int main() {
  std::string path = "./demo";
  std::string rm_cmd = "rm -rf " + path;
  system(rm_cmd.c_str());

  auto schema = create_schema();
  CollectionOptions options{false, true};

  auto result = Collection::CreateAndOpen(path, *schema, options);
  if (!result.has_value()) {
    std::cout << result.error().message() << std::endl;
    return -1;
  }

  std::cout << "Stats: " << result.value()->Stats().value().to_string()
            << std::endl;

  // insert docs
  { Doc doc; }

  // create index
  {}

  // query
  {}

  // insert more docs
  {}

  // optimize
  {}

  // close and reopen
  {}

  return 0;
}