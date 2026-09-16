import json
import sys

if __name__ == "__main__":
    with open(sys.argv[1], "rt", encoding="utf-8") as fr:
        with open(sys.argv[1]+".json", "wt", encoding="utf-8") as fw:
            fw.write(json.dumps(json.loads(fr.read()), ensure_ascii=False, indent=4))
        