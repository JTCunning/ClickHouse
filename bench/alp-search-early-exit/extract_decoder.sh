#!/usr/bin/env bash
# extract_decoder.sh <path to CompressionCodecALP.cpp> <out.h>: copies the STD and RD decoders of the real codec (unchanged by the branch).
awk '/^class ALPCodecDecoder/{print "template <FLOAT T>"; p=1} /^class ALPRDCodecEncoder/{p=0} /^class ALPRDCodecDecoder/{print "template <FLOAT T>"; p=1} /^CompressionCodecALP::CompressionCodecALP/{p=0} p' "$1" \
  | python3 -c 'import sys,re; s=sys.stdin.read(); s=re.sub(r"template <FLOAT T>\s*\Z","",s); s=re.sub(r"\}\s*\Z","",s.rstrip()); s=re.sub(r"template <FLOAT T>\s*(template <FLOAT T>)",r"\1",s); print(s)' > "$2"
