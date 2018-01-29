# KeyedFrames

A `KeyedFrame` is a `DataFrame` that also stores a vector of column names that together act
as a unique key.

When a `KeyedFrame` is joined with another `KeyedFrame` (or any other `AbstractDataFrame`),
the key can be used to determine which columns to join on. When a `KeyedFrame` is sorted, it
will sort by the key columns first by default.

## Constructor

```julia
KeyedFrame(df::DataFrame, key::Vector)
```

Create an `KeyedFrame` using the provided `DataFrame`; `key` specifies the columns
to use by default when performing a `join` on `KeyedFrame`s when `on` is not provided.

### Example

```julia
julia> kf1 = KeyedFrame(DataFrame(; a=1:10, b=2:11, c=3:12), [:a, :b])
10×3 KeyedFrames.KeyedFrame
│ Row │ a  │ b  │ c  │
├─────┼────┼────┼────┤
│ 1   │ 1  │ 2  │ 3  │
│ 2   │ 2  │ 3  │ 4  │
│ 3   │ 3  │ 4  │ 5  │
│ 4   │ 4  │ 5  │ 6  │
│ 5   │ 5  │ 6  │ 7  │
│ 6   │ 6  │ 7  │ 8  │
│ 7   │ 7  │ 8  │ 9  │
│ 8   │ 8  │ 9  │ 10 │
│ 9   │ 9  │ 10 │ 11 │
│ 10  │ 10 │ 11 │ 12 │

julia> kf2 = KeyedFrame(DataFrame(; a=[4, 2, 1], d=[2, 5, 2], e=1:3), [:d, :a])
3×3 KeyedFrames.KeyedFrame
│ Row │ a │ d │ e │
├─────┼───┼───┼───┤
│ 1   │ 4 │ 2 │ 1 │
│ 2   │ 2 │ 5 │ 2 │
│ 3   │ 1 │ 2 │ 3 │
```

## Joining

When performing a `join`, if only one of the arguments is an `KeyedFrame` and `on` is not
specified, the frames will be joined on the `key` of the `KeyedFrame`. If both
arguments are `KeyedFrame`s, `on` will default to the intersection of their respective
indices. In all cases, the result of the `join` will share a type with the first argument.

### Example

```julia
julia> join(kf1, kf2)
3×5 KeyedFrames.KeyedFrame
│ Row │ a │ b │ c │ d │ e │
├─────┼───┼───┼───┼───┼───┤
│ 1   │ 1 │ 2 │ 3 │ 2 │ 3 │
│ 2   │ 2 │ 3 │ 4 │ 5 │ 2 │
│ 3   │ 4 │ 5 │ 6 │ 2 │ 1 │

julia> keys(ans)
3-element Array{Symbol,1}:
 :a
 :b
 :d
```

Although the keys of both `KeyedFrame`s are used in constructing the default value for `on`,
the user may still supply the `on` keyword if they wish:

```julia
julia> join(kf1, kf2; on=[:a => :a, :b => :d], kind=:outer)
12×4 KeyedFrames.KeyedFrame
│ Row │ a  │ b  │ c       │ e       │
├─────┼────┼────┼─────────┼─────────┤
│ 1   │ 1  │ 2  │ 3       │ 3       │
│ 2   │ 2  │ 3  │ 4       │ missing │
│ 3   │ 3  │ 4  │ 5       │ missing │
│ 4   │ 4  │ 5  │ 6       │ missing │
│ 5   │ 5  │ 6  │ 7       │ missing │
│ 6   │ 6  │ 7  │ 8       │ missing │
│ 7   │ 7  │ 8  │ 9       │ missing │
│ 8   │ 8  │ 9  │ 10      │ missing │
│ 9   │ 9  │ 10 │ 11      │ missing │
│ 10  │ 10 │ 11 │ 12      │ missing │
│ 11  │ 4  │ 2  │ missing │ 1       │
│ 12  │ 2  │ 5  │ missing │ 2       │

julia> keys(ans)
2-element Array{Symbol,1}:
 :a
 :b
```

Notice that `:d` is no longer a key (as it has been renamed `:c`). It's important to note
that while the user may expect `:c` to be part of the new frame's key (as `:d` was), `join`
does not infer this.

## Sorting

When `sort`ing, if no `cols` keyword is supplied, the `key` is used to determine precedence.

```julia
julia> kf2
3×3 KeyedFrames.KeyedFrame
│ Row │ a │ d │ e │
├─────┼───┼───┼───┤
│ 1   │ 4 │ 2 │ 1 │
│ 2   │ 2 │ 5 │ 2 │
│ 3   │ 1 │ 2 │ 3 │

julia> keys(kf2)
2-element Array{Symbol,1}:
 :d
 :a

julia> sort(kf2)
3×3 KeyedFrames.KeyedFrame
│ Row │ a │ d │ e │
├─────┼───┼───┼───┤
│ 1   │ 1 │ 2 │ 3 │
│ 2   │ 4 │ 2 │ 1 │
│ 3   │ 2 │ 5 │ 2 │
```

## Equality

A `KeyedFrame` and a `DataFrame` with identical data are considered equal (`==` returns
`true`, though `isequal` will be false).

When testing for equality with `==`, `key` ordering is ignored, which means that it's
possible to have two `KeyedFrame`s that are considered equal but whose default sort order
will be different by virtue of having the columns listed in a different order in their
`key`s.
