#!/usr/bin/env python3


from operator import methodcaller
from typing import List
from sys import stderr, argv


from translator import Translator


def preprocess(lines: List[str]) -> List[List[object]]:
    def convert_type(itm: str):
        itm = itm.strip()
        try:
            return int(itm)
        except ValueError:
            pass
        if itm == "":
            return None
        elif itm.lower() in {"true", "fak"}:
            return True
        elif itm.lower() in {"false", "---"}:
            return False
        else:
            return itm

    lines = list(filter(None, lines))
    lines = list(map(methodcaller("split", "\t"), lines))
    for line in lines:
        if line[0].isnumeric():
            line.insert(0, "Trade")
    lines = list(map(lambda line: list(map(convert_type, line)), lines))
    return lines


def main():
    if len(argv) != 5:
        print("usage:\t%s <haskell_input> <haskell_output> <feed.mmtp> <oracle.mmtp>" % argv[0], file=stderr)
        exit(2)

    with open(argv[1]) as f:
        haskell_feed = list(map(str.strip, f))[2:]
    with open(argv[2]) as f:
        haskell_res = list(map(str.strip, f))
    request_count = int(haskell_res[0])
    haskell_res = haskell_res[1:]
    haskell_feed = preprocess(haskell_feed)
    haskell_res = preprocess(haskell_res)
    translator = Translator()
    translated_feed, translated_result = translator.translate(request_count, haskell_res)
    translated_feed = list(filter(None, translated_feed))
    translated_result = list(filter(None, translated_result))
    # print("\n".join(translated_feed))
    # print("\n".join(translated_result))
    with open(argv[3], "w") as f:
        print("\n".join(translated_feed), file=f)
    with open(argv[4], "w") as f:
        print("\n".join(translated_result), file=f)


if __name__ == '__main__':
    main()
