#!/usr/bin/env bash
# extract.sh <path to CompressionCodecALP.cpp> <out.h>: copies the encoder half of the real codec (constants, utils, STD and RD encoders).
src=$1; out=$2
awk '/^constexpr UInt8 ALP_CODEC_VERSION/{p=1} /^class ALPCodecDecoder/{p=0} /^class ALPRDCodecEncoder/{print "template <FLOAT T>"; p=1} /^class ALPRDCodecDecoder/{p=0} p' "$src" \
  | sed -e '/^template <FLOAT T>$/{N;/\n$/d}' > "$out.tmp"
# drop dangling "template <FLOAT T>" lines right before the decoder classes
python3 - "$out.tmp" "$out" <<'PY'
import sys,re
s=open(sys.argv[1]).read()
s=re.sub(r'template <FLOAT T>\s*\Z','',s)
s=re.sub(r'template <FLOAT T>\s*template <FLOAT T>','template <FLOAT T>',s)
open(sys.argv[2],'w').write(s)
PY
rm "$out.tmp"
