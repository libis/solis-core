require "test_helper"

class TestEntityInheritance < Minitest::Test

  def setup
    super
    @name_graph = 'https://example.com/'

    dir_tmp = File.join(__dir__, './data')

    @model_1 = Solis::Model.new(model: {
      uri: "file://test/resources/car/car_test_entity_inheritance.ttl",
      prefix: 'ex',
      namespace: @name_graph,
      tmp_dir: dir_tmp
    })

  end

  def test_entity_valid

    data = JSON.parse %(
      {
        "color": "green",
        "n_batteries": 1
      }
    )

    car = Solis::Model::Entity.new(data, @model_1, 'ElectricCar', nil)

    assert_equal(car.valid?, true)

  end

  def test_entity_invalid

    data = JSON.parse %(
      {
        "color": 12345,
        "n_batteries": 1
      }
    )

    car = Solis::Model::Entity.new(data, @model_1, 'ElectricCar', nil)

    assert_equal(car.valid?, false)

  end

end