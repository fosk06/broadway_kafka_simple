defmodule KafkaBroadwayTest do
  use ExUnit.Case
  doctest KafkaBroadwaySimple

  test "greets the world" do
    assert KafkaBroadwaySimple.hello() == :world
  end
end
