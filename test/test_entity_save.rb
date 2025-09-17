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

    repository = RDF::Repository.new
    store_1 = Solis::Store::RDFProxy.new(repository, @name_graph)
    store_2 = Solis::Store::RDFProxy.new('http://localhost:8890/sparql', @name_graph)

    @stores = [
      store_1,
      store_2
    ]
    store_2.logger.level = Logger::DEBUG

  end

  def test_entity_save_from_model_1

    @stores.each do |store|

      puts store.run_operations(store.delete_all)

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

      car = Solis::Model::Entity.new(data, @model_1, 'Car', store)

      car.save

      puts "\n\nREPO CONTENT:\n\n"
      puts store.as_repository.dump(:ntriples)

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
      puts store.as_repository.dump(:ntriples)

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

      graph_to_check = RDF::Graph.new(data: store.as_repository)
      delete_metadata_from_graph(graph_to_check)

      assert_equal(graph_truth.to_set == graph_to_check.to_set, true)

    end

  end

  def test_entity_save_from_model_2

    @stores.each do |store|

      puts store.run_operations(store.delete_all)

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

      car = Solis::Model::Entity.new(data, @model_2, 'ElectricCar', store)

      car.save

      puts "\n\nREPO CONTENT:\n\n"
      puts store.as_repository.dump(:ntriples)

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
      puts store.as_repository.dump(:ntriples)

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

      graph_to_check = RDF::Graph.new(data: store.as_repository)
      delete_metadata_from_graph(graph_to_check)

      assert_equal(graph_truth.to_set == graph_to_check.to_set, true)

    end

  end

  def test_entity_loads_refs_while_validate_or_save

    @stores.each do |store|

      puts store.run_operations(store.delete_all)

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

  end

  def test_entity_reset_array_and_resave

    @stores.each do |store|

      puts store.run_operations(store.delete_all)

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

      car = Solis::Model::Entity.new(data, @model_1, 'Car', store)

      car.save

      puts "\n\nREPO CONTENT:\n\n"
      puts store.as_repository.dump(:ntriples)

      car.attributes.owners = []

      car.save

      puts "\n\nREPO CONTENT:\n\n"
      puts store.as_repository.dump(:ntriples)

      puts car.to_pretty_json

    end

  end

  def test_entity_create_with_empty_object

    @stores.each do |store|

      puts store.run_operations(store.delete_all)

      data = JSON.parse %(
        {
          "_id": "https://example.com/dfd736c6-db76-44ed-b626-cdcec59b69f9",
          "name": "jon doe",
          "address": {}
        }
      )

      person = Solis::Model::Entity.new(data, @model_1, 'Person', store)

      person.save

      puts "\n\nREPO CONTENT:\n\n"
      puts store.as_repository.dump(:ntriples)

      puts person.to_pretty_json

    end

  end

  def test_entity_reset_object_and_resave

    @stores.each do |store|

      puts store.run_operations(store.delete_all)

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

      puts "\n\nREPO CONTENT:\n\n"
      puts store.as_repository.dump(:ntriples)

      person.attributes.address = {}

      person.save

      puts "\n\nREPO CONTENT:\n\n"
      puts store.as_repository.dump(:ntriples)

      puts person.to_pretty_json

    end

  end

  def test_entity_save_list

    @stores.each do |store|

      # NOTE: RDFProxy on RDF::Repository.new does not work fully with list save
      # (because of sparql-client, see https://github.com/ruby-rdf/sparql/issues/55).
      # Hence, will skip the following for that store.
      next if store.repository.is_a?(RDF::Repository)

      puts store.run_operations(store.delete_all)

      data = JSON.parse %(
        {
          "_id": "https://example.com/93b8781d-50de-47e2-a1dc-33cb641fd4be",
          "color": ["green", "yellow"],
          "comments": {
            "_list": [
              "nice in the beginning ...",
              "... lesser nice to drive after all"
            ]
          },
          "extra_comments": {
            "_list": [
              "a",
              "b",
              "c"
            ]
          },
          "owners": [
            {
              "_id": "https://example.com/dfd736c6-db76-44ed-b626-cdcec59b69f9",
              "name": "jon doe"
            }
          ]
        }
      )

      car = Solis::Model::Entity.new(data, @model_1, 'Car', store)

      puts car.to_pretty_pre_validate_jsonld
      puts car.valid?

      car.save

      puts "\n\nREPO CONTENT:\n\n"
      puts store.as_repository.dump(:ntriples)

      str_ttl_truth = %(
        <https://example.com/dfd736c6-db76-44ed-b626-cdcec59b69f9> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <https://example.com/Person> .
        <https://example.com/dfd736c6-db76-44ed-b626-cdcec59b69f9> <https://example.com/name> "jon doe" .
        <https://example.com/93b8781d-50de-47e2-a1dc-33cb641fd4be> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <https://example.com/Car> .
        <https://example.com/93b8781d-50de-47e2-a1dc-33cb641fd4be> <https://example.com/color> "green" .
        <https://example.com/93b8781d-50de-47e2-a1dc-33cb641fd4be> <https://example.com/color> "yellow" .
        <https://example.com/93b8781d-50de-47e2-a1dc-33cb641fd4be> <https://example.com/owners> <https://example.com/dfd736c6-db76-44ed-b626-cdcec59b69f9> .
        <https://example.com/93b8781d-50de-47e2-a1dc-33cb641fd4be> <https://example.com/comments> _:nodeID_b10428 .
        <https://example.com/93b8781d-50de-47e2-a1dc-33cb641fd4be> <https://example.com/extra_comments> _:nodeID_b10426 .
        _:nodeID_b10427 <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> "... lesser nice to drive after all" .
        _:nodeID_b10427 <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> <http://www.w3.org/1999/02/22-rdf-syntax-ns#nil> .
        _:nodeID_b10428 <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> "nice in the beginning ..." .
        _:nodeID_b10428 <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> _:nodeID_b10427 .
        _:nodeID_b10426 <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> "a" .
        _:nodeID_b10426 <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> _:nodeID_b10425 .
        _:nodeID_b10425 <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> "b" .
        _:nodeID_b10425 <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> _:nodeID_b10424 .
        _:nodeID_b10424 <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> "c" .
        _:nodeID_b10424 <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> <http://www.w3.org/1999/02/22-rdf-syntax-ns#nil> .
      )
      graph_truth = RDF::Graph.new
      graph_truth.from_ttl(str_ttl_truth)

      graph_to_check = RDF::Graph.new(data: store.as_repository)
      delete_metadata_from_graph(graph_to_check)
      
      assert_equal(graph_truth.isomorphic_with?(graph_to_check), true)

      car.attributes.comments = JSON.parse %(
        {
          "_list": [
            "bla bla 1",
            "bla bla 2",
            "bla bla 3"
          ]
        }
      )

      car.attributes.extra_comments = JSON.parse %(
        {
          "_list": [
            "d",
            "e"
          ]
        }
      )

      puts car.to_pretty_pre_validate_jsonld
      puts car.valid?

      car.save

      puts "\n\nREPO CONTENT:\n\n"
      puts store.as_repository.dump(:ntriples)

      str_ttl_truth = %(
      <https://example.com/dfd736c6-db76-44ed-b626-cdcec59b69f9> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <https://example.com/Person> .
      <https://example.com/dfd736c6-db76-44ed-b626-cdcec59b69f9> <https://example.com/name> "jon doe" .
      <https://example.com/93b8781d-50de-47e2-a1dc-33cb641fd4be> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <https://example.com/Car> .
      <https://example.com/93b8781d-50de-47e2-a1dc-33cb641fd4be> <https://example.com/color> "green" .
      <https://example.com/93b8781d-50de-47e2-a1dc-33cb641fd4be> <https://example.com/color> "yellow" .
      <https://example.com/93b8781d-50de-47e2-a1dc-33cb641fd4be> <https://example.com/owners> <https://example.com/dfd736c6-db76-44ed-b626-cdcec59b69f9> .
      <https://example.com/93b8781d-50de-47e2-a1dc-33cb641fd4be> <https://example.com/comments> _:nodeID_b10431 .
      <https://example.com/93b8781d-50de-47e2-a1dc-33cb641fd4be> <https://example.com/extra_comments> _:nodeID_b10430 .
      _:nodeID_b10431 <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> "bla bla 1" .
      _:nodeID_b10431 <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> _:nodeID_b10433 .
      _:nodeID_b10433 <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> "bla bla 2" .
      _:nodeID_b10433 <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> _:nodeID_b10432 .
      _:nodeID_b10432 <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> "bla bla 3" .
      _:nodeID_b10432 <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> <http://www.w3.org/1999/02/22-rdf-syntax-ns#nil> .
      _:nodeID_b10430 <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> "d" .
      _:nodeID_b10430 <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> _:nodeID_b10429 .
      _:nodeID_b10429 <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> "e" .
      _:nodeID_b10429 <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> <http://www.w3.org/1999/02/22-rdf-syntax-ns#nil> .
      )
      graph_truth = RDF::Graph.new
      graph_truth.from_ttl(str_ttl_truth)

      graph_to_check = RDF::Graph.new(data: store.as_repository)
      delete_metadata_from_graph(graph_to_check)

      assert_equal(graph_truth.isomorphic_with?(graph_to_check), true)

    end

  end

end

