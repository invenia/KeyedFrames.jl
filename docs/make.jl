using Documenter, KeyedFrame

makedocs(;
    modules=[KeyedFrame],
    format=:html,
    pages=[
        "Home" => "index.md",
    ],
    repo="https://github.com/invenia/KeyedFrame.jl/blob/{commit}{path}#L{line}",
    sitename="KeyedFrame.jl",
    authors="Invenia Technical Computing Corporation",
    assets=[
        "assets/invenia.css",
        "assets/logo.png",
    ],
)

deploydocs(;
    repo="github.com/invenia/KeyedFrame.jl",
    target="build",
    julia="0.6",
    deps=nothing,
    make=nothing,
)
