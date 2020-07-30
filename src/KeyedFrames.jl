module KeyedFrames

import Base: @deprecate
import DataFrames: deletecols!, deleterows!

using DataFrames
using DataFrames: DataFrameRow, SubDataFrame
using DataFrames: delete!, first, index, last, ncol, nonunique, nrow, permutecols!,
    rename, rename!, select, select!, unique!

struct KeyedFrame <: AbstractDataFrame
    frame::DataFrame
    key::Vector{Symbol}

    function KeyedFrame(df::DataFrame, key::Vector{Symbol})
        key = unique(key)
        df_names = propertynames(df)

        if !issubset(key, df_names)
            throw(
                ArgumentError(
                    string(
                        "The columns provided for the key ($key) must all be ",
                        "present in the DataFrame ($df_names)."
                    )
                )
            )
        end

        return new(df, key)
    end
end

function KeyedFrame(df::DataFrame, key::Vector{<:AbstractString})
    return KeyedFrame(df, map(Symbol, key))
end

KeyedFrame(df::DataFrame, key::Symbol) = KeyedFrame(df, [key])

"""
    KeyedFrame(df::DataFrame, key::Vector{Symbol})

Create an `KeyedFrame` using the provided `DataFrame`; `key` specifies the columns
to use by default when performing a `join` on `KeyedFrame`s when `on` is not provided.

When performing a `join`, if only one of the arguments is an `KeyedFrame` and `on` is not
specified, the frames will be joined on the `key` of the `KeyedFrame`. If both
arguments are `KeyedFrame`s, `on` will default to the intersection of their respective
indices. In all cases, the result of the `join` will share a type with the first argument.

When calling `unique` (or `unique!`) on a KeyedFrame without providing a `cols` argument,
`cols` will default to the `key` of the `KeyedFrame` instead of all columns. If you wish to
remove only rows that are duplicates across all columns (rather than just across the key),
you can call `unique!(kf, names(kf))`.

When `sort`ing, if no `cols` keyword is supplied, the `key` is used to determine precedence.

When testing for equality, `key` ordering is ignored, which means that it's possible to have
two `KeyedFrame`s that are considered equal but whose default sort order will be different
by virtue of having the columns listed in a different order in their `key`s.
"""
KeyedFrame

DataFrames.DataFrame(kf::KeyedFrame) = frame(kf)
Base.copy(kf::KeyedFrame) = KeyedFrame(copy(DataFrame(kf)), copy(keys(kf)))
Base.deepcopy(kf::KeyedFrame) = KeyedFrame(deepcopy(DataFrame(kf)), deepcopy(keys(kf)))

Base.convert(::Type{DataFrame}, kf::KeyedFrame) = frame(kf)

DataFrames.SubDataFrame(kf::KeyedFrame, args...) = SubDataFrame(frame(kf), args...)
DataFrames.DataFrameRow(kf::KeyedFrame, args...) = DataFrameRow(frame(kf), args...)

if isdefined(DataFrames, :_check_consistency)
    DataFrames._check_consistency(kf::KeyedFrame) = DataFrames._check_consistency(frame(kf))
end

##### EQUALITY #####

Base.:(==)(a::KeyedFrame, b::KeyedFrame) = frame(a) == frame(b) && sort(keys(a)) == sort(keys(b))

Base.isequal(a::KeyedFrame, b::KeyedFrame) = isequal(frame(a), frame(b)) && isequal(keys(a), keys(b))
Base.isequal(a::KeyedFrame, b::AbstractDataFrame) = false
Base.isequal(a::AbstractDataFrame, b::KeyedFrame) = false

Base.hash(kf::KeyedFrame, h::UInt) = hash(keys(kf), hash(frame(kf), h))

##### SIZE #####

DataFrames.nrow(kf::KeyedFrame) = nrow(frame(kf))
DataFrames.ncol(kf::KeyedFrame) = ncol(frame(kf))

##### ACCESSORS #####

DataFrames.index(kf::KeyedFrame) = index(frame(kf))
Base.names(kf::KeyedFrame) = names(frame(kf))

##### INDEXING #####

const ColumnIndex = Union{Real, Symbol}

frame(kf::KeyedFrame) = getfield(kf, :frame)
Base.keys(kf::KeyedFrame) = getfield(kf, :key)
Base.setindex!(kf::KeyedFrame, value, ind...) = setindex!(frame(kf), value, ind...)

# I don't want to have to write the same function body several times, so...
function _kf_getindex(kf::KeyedFrame, index...)
    # If indexing by column, some keys might be removed.
    df = frame(kf)[index...]
    return KeyedFrame(DataFrame(df), intersect(propertynames(df), keys(kf)))
end

# Returns a KeyedFrame
Base.getindex(kf::KeyedFrame, ::Colon) = copy(kf)
Base.getindex(kf::KeyedFrame, ::Colon, ::Colon) = copy(kf)

# Returns a KeyedFrame
Base.getindex(kf::KeyedFrame, ::typeof(!), col::AbstractVector) = _kf_getindex(kf, !, col)

# Returns a column
Base.getindex(kf::KeyedFrame, ::typeof(!), col::ColumnIndex) = frame(kf)[!, col]

# Returns a KeyedFrame or a column (depending on the type of col)
Base.getindex(kf::KeyedFrame, ::Colon, col) = frame(kf)[:, col]

# Returns a scalar
Base.getindex(kf::KeyedFrame, row::Integer, col::ColumnIndex) = frame(kf)[row, col]

# Returns a KeyedFrame
Base.getindex(kf::KeyedFrame, row::Integer, col::AbstractVector) = _kf_getindex(kf, row, col)

# Returns a column
Base.getindex(kf::KeyedFrame, row::AbstractVector, col::ColumnIndex) = frame(kf)[row, col]

# Returns a KeyedFrame
function Base.getindex(kf::KeyedFrame, row::AbstractVector, col::AbstractVector)
    return _kf_getindex(kf, row, col)
end

# Returns a KeyedFrame
function Base.getindex(kf::KeyedFrame, row::AbstractVector, col::Colon)
    return _kf_getindex(kf, row, col)
end

# Returns a KeyedFrame
Base.getindex(kf::KeyedFrame, row::Integer, col::Colon) = kf[[row], col]

##### SORTING #####

function Base.sort(kf::KeyedFrame, cols=nothing; kwargs...)
    return KeyedFrame(sort(frame(kf), cols === nothing ? keys(kf) : cols; kwargs...), keys(kf))
end

function Base.sort!(kf::KeyedFrame, cols=nothing; kwargs...)
    sort!(frame(kf), cols === nothing ? keys(kf) : cols; kwargs...)
    return kf
end

function Base.issorted(kf::KeyedFrame, cols=nothing; kwargs...)
    return issorted(frame(kf), cols === nothing ? keys(kf) : cols; kwargs...)
end

##### PUSH/APPEND/DELETE #####

function Base.push!(kf::KeyedFrame, data)
    push!(frame(kf), data)
    return kf
end

function Base.append!(kf::KeyedFrame, data)
    append!(frame(kf), data)
    return kf
end

function DataFrames.delete!(kf::KeyedFrame, inds)
    delete!(frame(kf), inds)
    return kf
end

@deprecate deleterows!(kf::KeyedFrame, inds) delete!(kf, inds)
@deprecate deletecols!(kf::KeyedFrame, inds) select!(kf, Not(inds))

function DataFrames.select!(kf::KeyedFrame, inds)
    select!(frame(kf), inds)
    new_keys = propertynames(kf)
    filter!(in(new_keys), keys(kf))
    return kf
end

function DataFrames.select(kf::KeyedFrame, inds; copycols::Bool=true)
    new_df = select(frame(kf), inds; copycols=copycols)
    df_names = propertynames(new_df)
    new_keys = filter(in(df_names), keys(kf))
    return KeyedFrame(new_df, new_keys)
end

##### RENAME #####

function DataFrames.rename!(kf::KeyedFrame, nms::AbstractVector{Pair{Symbol,Symbol}})
    rename!(frame(kf), nms)

    for (from, to) in nms
        i = findfirst(isequal(from), keys(kf))
        if i !== nothing
            keys(kf)[i] = to
        end
    end

    return kf
end

DataFrames.rename!(kf::KeyedFrame, nms::Pair{Symbol, Symbol}...) = rename!(kf, collect(nms))
DataFrames.rename!(kf::KeyedFrame, nms::Dict{Symbol, Symbol}) = rename!(kf, collect(pairs(nms)))
DataFrames.rename!(f::Function, kf::KeyedFrame) = rename!(kf, [(nm => f(nm)) for nm in propertynames(kf)])

DataFrames.rename(kf::KeyedFrame, args...) = rename!(copy(kf), args...)
DataFrames.rename(f::Function, kf::KeyedFrame) = rename!(f, copy(kf))

##### UNIQUE #####

_unique(kf::KeyedFrame, cols) = KeyedFrame(unique(frame(kf), cols), keys(kf))
function _unique!(kf::KeyedFrame, cols)
    unique!(frame(kf), cols)
    return kf
end

Base.unique(kf::KeyedFrame, cols::AbstractVector) = _unique(kf, cols)
Base.unique(kf::KeyedFrame, cols::Union{Integer, Symbol, Colon}) = _unique(kf, cols)
Base.unique(kf::KeyedFrame) = _unique(kf, keys(kf))
DataFrames.unique!(kf::KeyedFrame, cols::Union{Integer, Symbol, Colon}) = _unique!(kf, cols)
DataFrames.unique!(kf::KeyedFrame, cols::AbstractVector) = _unique!(kf, cols)
DataFrames.unique!(kf::KeyedFrame) = _unique!(kf, keys(kf))

DataFrames.nonunique(kf::KeyedFrame) = nonunique(frame(kf), keys(kf))
DataFrames.nonunique(kf::KeyedFrame, cols) = nonunique(frame(kf), cols)

##### JOINING #####

for j in (:innerjoin, :leftjoin, :rightjoin, :outerjoin, :semijoin, :antijoin, :crossjoin)

    # Note: We could probably support joining more than two DataFrames but it becomes
    # tricker with what key to use with multiple KeyedFrames.

    @eval begin
        # Returns a KeyedFrame
        function DataFrames.$j(
            kf1::KeyedFrame,
            kf2::KeyedFrame;
            on=nothing,
            kwargs...,
        )
            if on === nothing
                on = intersect(keys(kf1), keys(kf2))
            end

            result = $j(
                frame(kf1),
                frame(kf2);
                on=on,
                kwargs...,
            )

            key = $(if j in (:semijoin, :antijoin)
                :(intersect(keys(kf1), propertynames(result)))
            else
                # A join can sometimes rename columns, meaning some of the key columns "disappear"
                :(intersect(union(keys(kf1), keys(kf2)), propertynames(result)))
            end)

            return KeyedFrame(result, key)
        end

        # Returns a KeyedFrame
        function DataFrames.$j(
            kf::KeyedFrame,
            df::AbstractDataFrame;
            on=nothing,
            kwargs...,
        )
            if on === nothing
                on = intersect(keys(kf), propertynames(df))
            end

            result = $j(
                frame(kf),
                df;
                on=on,
                kwargs...,
            )

            key = intersect(keys(kf), propertynames(result))

            return KeyedFrame(result, key)
        end

        # Does NOT return a KeyedFrame
        function DataFrames.$j(
            df::AbstractDataFrame,
            kf::KeyedFrame;
            on=nothing,
            kwargs...,
        )
            if on === nothing
                on = intersect(keys(kf), propertynames(df))
            end

            result = $j(
                df,
                frame(kf);
                on=on,
                kwargs...,
            )

            return result
        end
    end
end

for (T, S) in [
    (:KeyedFrame, :KeyedFrame),
    (:KeyedFrame, :AbstractDataFrame),
    (:AbstractDataFrame, :KeyedFrame)
]
    @eval begin
        function Base.join(df1::$T, df2::$S; on=nothing, kind=:inner, kwargs...)
            j = if kind === :inner
                innerjoin
            elseif kind === :left
                leftjoin
            elseif kind === :right
                rightjoin
            elseif kind === :outer
                outerjoin
            elseif kind === :semi
                semijoin
            elseif kind === :anti
                antijoin
            elseif kind === :crossjoin
                crossjoin
            else
                throw(ArgumentError("Unknown join kind: $kind"))
            end

            Base.depwarn("$kind joining data frames using `join` is deprecated, use `$(kind)join` instead", :join)

            return j(df1, df2; on=on, kwargs...)
        end
    end
end

##### FIRST/LAST #####

DataFrames.first(kf::KeyedFrame, r::Int) = KeyedFrame(first(frame(kf), r), keys(kf))
DataFrames.last(kf::KeyedFrame, r::Int) = KeyedFrame(last(frame(kf), r), keys(kf))

##### PERMUTE #####

function DataFrames.permutecols!(kf::KeyedFrame, index::AbstractVector)
    select!(frame(kf), index)
    return kf
end

export KeyedFrame

end
