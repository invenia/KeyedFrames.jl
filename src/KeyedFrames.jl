__precompile__()
module KeyedFrames

using DataFrames
import DataFrames: SubDataFrame, nrow, ncol, index, deleterows!, delete!, rename!, rename,
       unique!, nonunique, head, tail

struct KeyedFrame <: AbstractDataFrame
    frame::DataFrame
    key::Vector{Symbol}

    function KeyedFrame(df::DataFrame, key::Vector{<:Symbol})
        key = unique(key)

        if !issubset(key, names(df))
            throw(
                ArgumentError(
                    "The columns provided for the key must all be present in the DataFrame"
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

Base.copy(kf::KeyedFrame) = KeyedFrame(copy(DataFrame(kf)), copy(keys(kf)))
Base.deepcopy(kf::KeyedFrame) = KeyedFrame(deepcopy(DataFrame(kf)), deepcopy(keys(kf)))

Base.convert(::Type{DataFrame}, kf::KeyedFrame) = kf.frame

SubDataFrame(kf::KeyedFrame, args...) = SubDataFrame(kf.frame, args...)

##### EQUALITY #####

Base.:(==)(a::KeyedFrame, b::KeyedFrame) = a.frame == b.frame && sort(a.key) == sort(b.key)

Base.isequal(a::KeyedFrame, b::KeyedFrame) = isequal(a.frame,b.frame)&&isequal(a.key,b.key)
Base.isequal(a::KeyedFrame, b::AbstractDataFrame) = false
Base.isequal(a::AbstractDataFrame, b::KeyedFrame) = false

Base.hash(kf::KeyedFrame, h::UInt) = hash(kf.key, hash(kf.frame, h))

##### SIZE #####

nrow(kf::KeyedFrame) = nrow(kf.frame)
ncol(kf::KeyedFrame) = ncol(kf.frame)

##### ACCESSORS #####

index(kf::KeyedFrame) = index(kf.frame)
Base.names(kf::KeyedFrame) = names(kf.frame)

##### INDEXING #####

const ColumnIndex = Union{Real, Symbol}

Base.keys(kf::KeyedFrame) = kf.key
Base.setindex!(kf::KeyedFrame, value, ind...) = setindex!(kf.frame, value, ind...)

# I don't want to have to write the same function body several times, so...
function _kf_getindex(kf::KeyedFrame, index...)
    # If indexing by column, some keys might be removed.
    df = getindex(kf.frame, index...)
    return KeyedFrame(df, intersect(names(df), kf.key))
end

# Returns a KeyedFrame
Base.getindex(kf::KeyedFrame, ::Colon) = copy(kf)
Base.getindex(kf::KeyedFrame, ::Colon, ::Colon) = copy(kf)

# Returns a KeyedFrame
Base.getindex(kf::KeyedFrame, col::AbstractVector) = _kf_getindex(kf, col)

# Returns a column
Base.getindex(kf::KeyedFrame, col::ColumnIndex) = kf.frame[col]

# Returns a KeyedFrame or a column (depending on the type of col)
Base.getindex(kf::KeyedFrame, ::Colon, col) = kf[col]

# Returns a scalar
Base.getindex(kf::KeyedFrame, row::Real, col::ColumnIndex) = kf.frame[row, col]

# Returns a KeyedFrame
Base.getindex(kf::KeyedFrame, row::Real, col::AbstractVector) = _kf_getindex(kf, row, col)

# Returns a column
Base.getindex(kf::KeyedFrame, row::AbstractVector, col::ColumnIndex) = kf.frame[row, col]

# Returns a KeyedFrame
function Base.getindex(kf::KeyedFrame, row::AbstractVector, col::AbstractVector)
    return _kf_getindex(kf, row, col)
end

# Returns a KeyedFrame
function Base.getindex(kf::KeyedFrame, row::AbstractVector, col::Colon)
    return _kf_getindex(kf, row, col)
end

# Returns a KeyedFrame
Base.getindex(kf::KeyedFrame, row::Real, col::Colon) = kf[[row], col]

##### SORTING #####

function Base.sort(kf::KeyedFrame, cols=nothing; kwargs...)
    return KeyedFrame(sort(kf.frame, cols === nothing ? kf.key : cols; kwargs...), kf.key)
end

function Base.sort!(kf::KeyedFrame, cols=nothing; kwargs...)
    sort!(kf.frame, cols === nothing ? kf.key : cols; kwargs...)
    return kf
end

function Base.issorted(kf::KeyedFrame, cols=nothing; kwargs...)
    return issorted(kf.frame, cols === nothing ? kf.key : cols; kwargs...)
end

##### PUSH/APPEND/DELETE #####

Base.push!(kf::KeyedFrame, data) = push!(kf.frame, data)
Base.append!(kf::KeyedFrame, data) = append!(kf.frame, data)

deleterows!(kf::KeyedFrame, ind) = deleterows!(kf.frame, ind)

delete!(kf::KeyedFrame, ind::Union{Integer, Symbol}) = delete!(kf, [ind])
delete!(kf::KeyedFrame, ind::Vector{<:Integer}) = delete!(kf, names(kf)[ind])

function delete!(kf::KeyedFrame, ind::Vector{<:Symbol})
    delete!(kf.frame, ind)
    filter!(x -> !in(x, ind), kf.key)
    return kf
end

##### RENAME #####

function rename!(kf::KeyedFrame, nms)
    rename!(kf.frame, nms)

    for (from, to) in nms
        i = findfirst(kf.key, from)
        if i != 0
            kf.key[i] = to
        end
    end

    return kf
end

rename!(kf::KeyedFrame, nms::Pair{Symbol, Symbol}...) = rename!(kf, collect(nms))
rename!(f::Function, kf::KeyedFrame) = rename!(kf, [(nm => f(nm)) for nm in names(kf)])

rename(kf::KeyedFrame, args...) = rename!(copy(kf), args...)
rename(f::Function, kf::KeyedFrame) = rename!(f, copy(kf))

##### UNIQUE #####

function Base.unique(kf::KeyedFrame, cols=nothing)
    return KeyedFrame(unique(kf.frame, cols === nothing ? kf.key : cols), kf.key)
end

function unique!(kf::KeyedFrame, cols=nothing)
    unique!(kf.frame, cols === nothing ? kf.key : cols)
    return kf
end

nonunique(kf::KeyedFrame) = nonunique(kf.frame, kf.key)

##### JOIN #####

# Returns a KeyedFrame
function Base.join(a::KeyedFrame, b::KeyedFrame; on=nothing, kind=:inner, kwargs...)
    df = join(
        a.frame,
        b.frame;
        on=on === nothing ? intersect(a.key, b.key) : on,
        kind=kind,
        kwargs...,
    )

    if kind in (:semi, :anti)
        key = intersect(a.key, names(df))
    else
        # A join can sometimes rename columns, meaning some of the key columns "disappear"
        key = intersect(union(a.key, b.key), names(df))
    end

    return KeyedFrame(df, key)
end

# Returns a KeyedFrame
function Base.join(a::KeyedFrame, b::AbstractDataFrame; on=nothing, kwargs...)
    df = join(a.frame, b; on=on === nothing ? intersect(a.key, names(b)) : on, kwargs...)

    # A join can sometimes rename columns, meaning some of the key columns "disappear"
    return KeyedFrame(df, intersect(a.key, names(df)))
end

# Does NOT return a KeyedFrame
function Base.join(a::AbstractDataFrame, b::KeyedFrame; on=nothing, kwargs...)
    return join(a, b.frame; on=on === nothing ? intersect(b.key, names(a)) : on, kwargs...)
end

##### HEAD/TAIL #####

head(kf::KeyedFrame, r::Int) = KeyedFrame(head(kf.frame, r), kf.key)
tail(kf::KeyedFrame, r::Int) = KeyedFrame(tail(kf.frame, r), kf.key)

##### PERMUTE #####

function Base.permute!(df::DataFrame, index::AbstractVector)
    permute!(DataFrames.columns(df), index)
    df.colindex = DataFrames.Index(
        Dict(names(df)[j] => i for (i, j) in enumerate(index)),
        [names(df)[j] for j in index]
    )
end

Base.permute!(kf::KeyedFrame, index::AbstractVector) = permute!(kf.frame, index)

export KeyedFrame

end
