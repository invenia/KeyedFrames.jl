# KeyedFrames

[![Stable](https://img.shields.io/badge/docs-stable-blue.svg)](https://invenia.github.io/KeyedFrames.jl/stable)
[![Latest](https://img.shields.io/badge/docs-latest-blue.svg)](https://invenia.github.io/KeyedFrames.jl/latest)
[![CI](https://github.com/Invenia/KeyedFrames.jl/workflows/CI/badge.svg)](https://github.com/Invenia/KeyedFrames.jl/actions?query=workflow%3ACI)
[![CodeCov](https://codecov.io/gh/invenia/KeyedFrames.jl/branch/master/graph/badge.svg)](https://codecov.io/gh/invenia/KeyedFrames.jl)

A `KeyedFrame` is a `DataFrame` that also stores a vector of column names that together act
as a unique key, which can be used to determine which columns to `join`, `unique`, and
`sort` on by default.
