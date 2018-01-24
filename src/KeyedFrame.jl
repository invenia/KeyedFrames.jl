__precompile__()
module KeyedFrame

using DataFrames
import DataFrames: SubDataFrame, OnType, nrow, ncol, index, getindex, setindex!, sort!,
       push!, append!, deleterows!, head, tail

struct KeyedFrame <: AbstractDataFrame
    frame::DataFrame
    key::Vector{<:OnType}

    function KeyedFrame(df::DataFrame, key::Vector{<:OnType})
        if !issubset(key, names(df))
            throw(
                ArgumentError(
                    "The index columns provided must all be present in the DataFrame."
                )
            )
        end

        return new(df, key)
    end
end

function KeyedFrame(df::DataFrame, key::Vector{<:AbstractString})
    return KeyedFrame(df, map(Symbol, key))
end

KeyedFrame(df::DataFrame, key::OnType) = KeyedFrame(df, [key])

"""
    KeyedFrame(df::DataFrame, key::Vector)

Create an `KeyedFrame` using the provided `DataFrame`; `key` specifies the columns
to use by default when performing a `join` on `KeyedFrame`s when `on` is not provided.

When performing a `join`, if only one of the arguments is an `KeyedFrame` and `on` is not
specified, the frames will be joined on the `key` of the `KeyedFrame`. If both
arguments are `KeyedFrame`s, `on` will default to the intersection of their respective
indices. In all cases, the result of the `join` will share a type with the first argument.
"""
KeyedFrame

Base.convert(::Type{DataFrame}, frame::KeyedFrame) = frame.df

nrow(kf::KeyedFrame) = nrow(kf.frame)
ncol(kf::KeyedFrame) = ncol(kf.frame)

index(kf::KeyedFrame) = index(kf.frame)
getindex(kf::KeyedFrame, indices...) = getindex(kf.frame, indices...)
setindex!(kf::KeyedFrame, value, ind...) = setindex!(kf.frame, value, ind...)
key(kf::KeyedFrame) = kf.key

sort!(kf::KeyedFrame; kwargs...) = sort!(kf.frame; kwargs...)
SubDataFrame(kf::KeyedFrame, args...) = SubDataFrame(kf.frame, args...)

push!(kf::KeyedFrame, data) = push!(kf.frame, data)
append!(kf::KeyedFrame, data) = append!(kf.frame, data)
deleterows!(kf::KeyedFrame, ind) = deleterows!(kf.frame, ind)

# Returns an KeyedFrame
function Base.join(a::KeyedFrame, b::KeyedFrame; on=nothing, kwargs...)
    key = intersect(a.key, b.key)
    return KeyedFrame(join(a.frame, b.frame; on=on === nothing ? key : on, kwargs...), key)
end

# Returns an KeyedFrame
function Base.join(a::KeyedFrame, b::AbstractDataFrame; on=nothing, kwargs...)
    return KeyedFrame(
        join(a.frame, b.frame; on=on === nothing ? a.key : on, kwargs...), a.key
    )
end

# Does NOT return an KeyedFrame
function Base.join(a::AbstractDataFrame, b::KeyedFrame; on=nothing, kwargs...)
    return join(a.frame, b.frame; on=on === nothing ? b.key : on, kwargs...)
end

head(kf::KeyedFrame, args...) = KeyedFrame(head(kf.frame, args...), kf.key)
tail(kf::KeyedFrame, args...) = KeyedFrame(tail(kf.frame, args...), kf.key)

function Base.permute!(df::DataFrame, index::AbstractVector)
    permute!(df.columns, index)
    df.colindex = DataFrames.Index(
        Dict(df.colindex.names[j] => i for (i, j) in enumerate(index)),
        [df.colindex.names[j] for j in index]
    )
end

Base.permute!(kf::KeyedFrame, index::AbstractVector) = permute!(kf.frame, index)

export KeyedFrame, key

end
