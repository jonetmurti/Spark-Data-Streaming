import os
import argparse
import json
from tqdm import tqdm 

def load_json(file_path):
  print(file_path)
  with open(file_path, encoding="utf8") as f:
    return json.load(f)

def get_json_value(data, key):
  keys = key.split(".")
  if len(keys) == 1:
    if keys[0] in data.keys():
      return data[keys[0]]
    else:
      return None
  else:
    currentVal = data
    for key in keys:
      if key in currentVal.keys():
        currentVal = currentVal[key]
      else:
        return None
    return currentVal
  return None

def extractor(file_prefix, dump_path, data_path, key):
  extracted = []
  key_list = key.split("-")
  field_list = ["original_id", "content", "from_id", "from_name", "created_at", "social_media"]
  for file in tqdm(os.listdir(data_path)):
    if not file_prefix in file:
      continue
    filenames = file.split("_")
    dataset = load_json(os.path.join(data_path, file))
    for data in dataset:
      extracted_data = {}
      for i in range(len(key_list)):
        extracted_data[field_list[i]] = get_json_value(data, key_list[i])
      extracted_data["social_media"] = filenames[0]
      extracted.append(extracted_data)
  with open(os.path.join(dump_path, f"{file_prefix}.json"), "w+") as f:
    json.dump(extracted, f)
  print("Extract Process Done !")

if __name__ == "__main__":
  parser = argparse.ArgumentParser(
    description='Extract social media json data')
  parser.add_argument('--file_prefix', type=str, default='facebook',
    help='Prefix of to be extracted file')
  parser.add_argument('--dump_path', type=str, default='./res',
    help='Path to output')
  parser.add_argument('--data_path', type=str, default='raw_json',
    help='data path')
  parser.add_argument('--key', type=str, default='./raw_json',
    help='json key needs to be extracted, separated by _ and chained with .')
  args = parser.parse_args()

  extractor(file_prefix=args.file_prefix, dump_path=args.dump_path, data_path=args.data_path, key=args.key)
  
  