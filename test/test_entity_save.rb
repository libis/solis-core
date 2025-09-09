require "test_helper"

class TestEntitySave < Minitest::Test

  def setup
    super
    @name_graph = 'https://example.com/'

    dir_tmp = File.join(__dir__, './data')

    @model_1 = Solis::Model.new(model: {
      uri: "file://test/resources/car/car_test_entity_save.ttl",
      prefix: 'ex',
      namespace: @name_graph,
      tmp_dir: dir_tmp
    })

    hierarchy = {
      'ElectricCar' => ['Car']
    }
    @model_2 = Solis::Model.new(model: {
                                  uri: "file://test/resources/car/car_test_entity_save.ttl",
                                  prefix: 'ex',
                                  namespace: @name_graph,
                                  tmp_dir: dir_tmp,
                                  hierarchy: hierarchy
                                })

  end

  def test_entity_save_from_model_1

    data = JSON.parse %(
      {
        "_id": "https://example.com/93b8781d-50de-47e2-a1dc-33cb641fd4be",
        "color": ["green", "yellow"],
        "brand": "toyota",
        "owners": [
          {
            "_id": "https://example.com/dfd736c6-db76-44ed-b626-cdcec59b69f9",
            "name": "jon doe",
            "address": {
              "_id": "https://example.com/3117582b-cdef-4795-992f-b62efd8bb1ea",
              "street": "fake street"
            }
          }
        ]
      }
    )

    repository = RDF::Repository.new
    store = Solis::Store::RDFProxy.new(repository, @name_graph)

    car = Solis::Model::Entity.new(data, @model_1, 'Car', store)

    car.save

    puts "\n\nREPO CONTENT:\n\n"
    puts repository.dump(:ntriples)

    obj_patch = JSON.parse %(
      {
        "color": "black",
        "brand": "@unset",
        "owners": [
          {
            "_id": "https://example.com/dfd736c6-db76-44ed-b626-cdcec59b69f9",
            "name": "john smith"
          }
        ]
      }
    )

    car.patch(obj_patch)

    car.save

    puts "\n\nREPO CONTENT:\n\n"
    puts repository.dump(:ntriples)

    str_ttl_truth = %(
      <https://example.com/3117582b-cdef-4795-992f-b62efd8bb1ea> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <https://example.com/Address> .
      <https://example.com/3117582b-cdef-4795-992f-b62efd8bb1ea> <https://example.com/street> "fake street" .
      <https://example.com/dfd736c6-db76-44ed-b626-cdcec59b69f9> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <https://example.com/Person> .
      <https://example.com/dfd736c6-db76-44ed-b626-cdcec59b69f9> <https://example.com/address> <https://example.com/3117582b-cdef-4795-992f-b62efd8bb1ea> .
      <https://example.com/dfd736c6-db76-44ed-b626-cdcec59b69f9> <https://example.com/name> "john smith" .
      <https://example.com/93b8781d-50de-47e2-a1dc-33cb641fd4be> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <https://example.com/Car> .
      <https://example.com/93b8781d-50de-47e2-a1dc-33cb641fd4be> <https://example.com/owners> <https://example.com/dfd736c6-db76-44ed-b626-cdcec59b69f9> .
      <https://example.com/93b8781d-50de-47e2-a1dc-33cb641fd4be> <https://example.com/color> "black" .
    )
    graph_truth = RDF::Graph.new
    graph_truth.from_ttl(str_ttl_truth)

    graph_to_check = RDF::Graph.new(data: repository)
    delete_metadata_from_graph(graph_to_check)

    assert_equal(graph_truth == graph_to_check, true)

  end

  def test_entity_save_from_model_2

    data = JSON.parse %(
      {
        "_id": "https://example.com/93b8781d-50de-47e2-a1dc-33cb641fd4be",
        "color": ["green", "yellow"],
        "brand": "toyota",
        "owners": [
          {
            "_id": "https://example.com/dfd736c6-db76-44ed-b626-cdcec59b69f9",
            "name": "jon doe",
            "address": {
              "_id": "https://example.com/3117582b-cdef-4795-992f-b62efd8bb1ea",
              "street": "fake street"
            }
          }
        ]
      }
    )

    repository = RDF::Repository.new
    store = Solis::Store::RDFProxy.new(repository, @name_graph)

    car = Solis::Model::Entity.new(data, @model_2, 'ElectricCar', store)

    car.save

    puts "\n\nREPO CONTENT:\n\n"
    puts repository.dump(:ntriples)

    obj_patch = JSON.parse %(
      {
        "color": "black",
        "brand": "@unset",
        "owners": [
          {
            "_id": "https://example.com/dfd736c6-db76-44ed-b626-cdcec59b69f9",
            "name": "john smith"
          }
        ]
      }
    )

    car.patch(obj_patch)

    car.save

    puts "\n\nREPO CONTENT:\n\n"
    puts repository.dump(:ntriples)

    str_ttl_truth = %(
      <https://example.com/3117582b-cdef-4795-992f-b62efd8bb1ea> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <https://example.com/Address> .
      <https://example.com/3117582b-cdef-4795-992f-b62efd8bb1ea> <https://example.com/street> "fake street" .
      <https://example.com/dfd736c6-db76-44ed-b626-cdcec59b69f9> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <https://example.com/Person> .
      <https://example.com/dfd736c6-db76-44ed-b626-cdcec59b69f9> <https://example.com/address> <https://example.com/3117582b-cdef-4795-992f-b62efd8bb1ea> .
      <https://example.com/dfd736c6-db76-44ed-b626-cdcec59b69f9> <https://example.com/name> "john smith" .
      <https://example.com/93b8781d-50de-47e2-a1dc-33cb641fd4be> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <https://example.com/ElectricCar> .
      <https://example.com/93b8781d-50de-47e2-a1dc-33cb641fd4be> <https://example.com/owners> <https://example.com/dfd736c6-db76-44ed-b626-cdcec59b69f9> .
      <https://example.com/93b8781d-50de-47e2-a1dc-33cb641fd4be> <https://example.com/color> "black" .
    )

    graph_truth = RDF::Graph.new
    graph_truth.from_ttl(str_ttl_truth)

    graph_to_check = RDF::Graph.new(data: repository)
    delete_metadata_from_graph(graph_to_check)

    assert_equal(graph_truth == graph_to_check, true)

  end

  def test_entity_loads_refs_while_validate_or_save

    repository = RDF::Repository.new
    store = Solis::Store::RDFProxy.new(repository, @name_graph)

    data = JSON.parse %(
      {
        "_id": "https://example.com/dfd736c6-db76-44ed-b626-cdcec59b69f9",
        "name": "jon doe",
        "address": {
          "_id": "https://example.com/3117582b-cdef-4795-992f-b62efd8bb1ea",
          "street": "fake street"
        }
      }
    )

    person = Solis::Model::Entity.new(data, @model_1, 'Person', store)

    person.save

    data = JSON.parse %(
      {
        "_id": "https://example.com/93b8781d-50de-47e2-a1dc-33cb641fd4be",
        "color": ["green", "yellow"],
        "brand": "toyota",
        "owners": [
          {
            "_id": "https://example.com/dfd736c6-db76-44ed-b626-cdcec59b69f9"
          }
        ]
      }
    )

    car = Solis::Model::Entity.new(data, @model_1, 'Car', store)

    assert_equal(car.valid?, true)

    car.save

  end

  def test_entity_reset_array_and_resave

    data = JSON.parse %(
      {
        "_id": "https://example.com/93b8781d-50de-47e2-a1dc-33cb641fd4be",
        "color": ["green", "yellow"],
        "brand": "toyota",
        "owners": [
          {
            "_id": "https://example.com/dfd736c6-db76-44ed-b626-cdcec59b69f9",
            "name": "jon doe",
            "address": {
              "_id": "https://example.com/3117582b-cdef-4795-992f-b62efd8bb1ea",
              "street": "fake street"
            }
          }
        ]
      }
    )

    repository = RDF::Repository.new
    store = Solis::Store::RDFProxy.new(repository, @name_graph)

    car = Solis::Model::Entity.new(data, @model_1, 'Car', store)

    car.save

    puts "\n\nREPO CONTENT:\n\n"
    puts repository.dump(:ntriples)

    car.attributes.owners = []

    car.save

    puts "\n\nREPO CONTENT:\n\n"
    puts repository.dump(:ntriples)

    puts car.to_pretty_json

  end

  def test_entity_create_with_empty_object

    data = JSON.parse %(
      {
        "_id": "https://example.com/dfd736c6-db76-44ed-b626-cdcec59b69f9",
        "name": "jon doe",
        "address": {}
      }
    )

    repository = RDF::Repository.new
    store = Solis::Store::RDFProxy.new(repository, @name_graph)

    @model_1.logger.level = Logger::DEBUG
    store.logger.level = Logger::DEBUG

    person = Solis::Model::Entity.new(data, @model_1, 'Person', store)

    person.save

    puts "\n\nREPO CONTENT:\n\n"
    puts repository.dump(:ntriples)

    puts person.to_pretty_json

  end

  def test_entity_reset_object_and_resave

    data = JSON.parse %(
      {
        "_id": "https://example.com/dfd736c6-db76-44ed-b626-cdcec59b69f9",
        "name": "jon doe",
        "address": {
          "_id": "https://example.com/3117582b-cdef-4795-992f-b62efd8bb1ea",
          "street": "fake street"
        }
      }
    )

    repository = RDF::Repository.new
    store = Solis::Store::RDFProxy.new(repository, @name_graph)

    person = Solis::Model::Entity.new(data, @model_1, 'Person', store)

    person.save

    puts "\n\nREPO CONTENT:\n\n"
    puts repository.dump(:ntriples)

    person.attributes.address = {}

    @model_1.logger.level = Logger::DEBUG
    store.logger.level = Logger::DEBUG

    person.save

    puts "\n\nREPO CONTENT:\n\n"
    puts repository.dump(:ntriples)

    puts person.to_pretty_json

  end

  def test_entity_save_list

    data = JSON.parse %(
      {
        "_id": "https://example.com/93b8781d-50de-47e2-a1dc-33cb641fd4be",
        "color": ["green", "yellow"],
        "comments": {
          "_list": [
            "nice in the beginning ...",
            "... lesser nice to drive after all"
          ]
        }
      }
    )

    repository = RDF::Repository.new
    store = Solis::Store::RDFProxy.new(repository, @name_graph)

    car = Solis::Model::Entity.new(data, @model_1, 'Car', store)

    puts car.to_pretty_pre_validate_jsonld
    puts car.valid?

    car.save

    puts "\n\nREPO CONTENT:\n\n"
    puts repository.dump(:ntriples)

    # NOTE: uncomment following when list deletion in triple store works.

    # car.attributes.comments = JSON.parse %(
    #   {
    #     "_list": [
    #       "bla bla 1",
    #       "bla bla 2",
    #       "bla bla 3"
    #     ]
    #   }
    # )
    #
    # puts car.to_pretty_pre_validate_jsonld
    # puts car.valid?
    #
    # car.save
    #
    # puts "\n\nREPO CONTENT:\n\n"
    # puts repository.dump(:ntriples)

  end

end

