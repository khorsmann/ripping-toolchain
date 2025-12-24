#!/usr/bin/env bash

out=chapters.txt
echo "" > "$out"

ts="00:00:00.000"
i=1

for f in *.mkv; do
  dur=$(ffprobe -v error -show_entries format=duration \
        -of default=noprint_wrappers=1:nokey=1 "$f")

  printf "CHAPTER%02d=%s\n" "$i" "$ts" >> "$out"
  printf "CHAPTER%02dNAME=%s\n" "$i" "${f%.mkv}" >> "$out"

  ts=$(python3 - <<EOF
import datetime
t=datetime.timedelta(seconds=float("$dur"))
h,m,s=str(t).split(':')
print(f"{int(h):02d}:{int(m):02d}:{float(s):06.3f}")
EOF
)
  ((i++))
done

# mkvpropedit komplett.mkv --chapters chapters.txt
