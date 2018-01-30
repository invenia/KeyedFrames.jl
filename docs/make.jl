using Documenter, KeyedFrames

makedocs(;
    modules=[KeyedFrames],
    format=:html,
    pages=[
        "Home" => "index.md",
    ],
    repo="https://github.com/invenia/KeyedFrames.jl/blob/{commit}{path}#L{line}",
    sitename="KeyedFrames.jl",
    authors="Invenia Technical Computing Corporation",
    assets=[
        "assets/invenia.css",
        "assets/logo.png",
    ],
)

deploydocs(;
    repo="github.com/invenia/KeyedFrames.jl",
    target="build",
    julia="0.6",
    deps=nothing,
    make=nothing,
)
