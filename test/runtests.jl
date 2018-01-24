using KeyedFrame
using Base.Test

@testset "KeyedFrame" begin
    df1 = DataFrame(a=1:10, b=2:11, c=3:12)
    df2 = DataFrame(a=1:5, d=4:8)
    df3 = DataFrame(a=1:5, b=2:6, e=5:9)

    @testset "constructor" begin
        @test false
    end

    @testset "convert" begin
        @test false
    end

    @testset "size" begin
        @test false
    end

    @testset "index" begin
        @test false
    end

    @testset "size" begin
        @test false
    end

    @testset "sort" begin
        @test false
    end

    @testset "modify" begin
        @test false
    end

    @testset "join" begin
        # TODO Test join with both index frames and half index frames
        @test false
    end

    @testset "head/tail" begin
        @test false
    end

    @testset "permute" begin
        @test false
    end
end
