SELECT bitShiftRight(reinterpretAsUInt128(timeSeriesLocalityId('mm', map('b', '1'))), 64)
    = bitShiftRight(reinterpretAsUInt128(timeSeriesLocalityId('mm', map('b', '2'))), 64);
SELECT timeSeriesLocalityId('m', map('k', '1')) != timeSeriesLocalityId('m', map('k', '2'));
SELECT reinterpretAsUInt128(timeSeriesLocalityId('a', map())) < reinterpretAsUInt128(timeSeriesLocalityId('b', map()));
SELECT hex(timeSeriesLocalityId('m', map('job', 'x', 'k', 'v'))) = hex(timeSeriesLocalityId('m', 'job', 'x', map('k', 'v')));
