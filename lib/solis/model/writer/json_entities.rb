require 'json'
require_relative "../parser/shacl"

class JSONEntitiesWriter < Solis::Model::Writer::Generic
  def self.write(repository, options = {})

    return "No repository provided" if repository.nil?
    return "options[:shapes] missing" unless options.key?(:shapes)

    raw = options[:raw] || false

    shapes = options[:shapes]
    context_inv = options[:context_inv]

    graph_namespace = options[:namespace]
    graph_title = options[:title]
    graph_version = options[:version]
    graph_version_counter = options[:version_counter]
    graph_description = options[:description]

    entities = {}

    names_entities = Shapes.get_all_classes(shapes)
    names_entities.each do |name_entity|
      # NOTE: infer CLASS property from SHAPE property.
      # If multiple shapes have the same target class (rare but can happen ...), just take one value.
      names_shapes = Shapes.get_shapes_for_class(shapes, name_entity)
      names = names_shapes.collect { |s| shapes[s][:name] }
      name = names[0]
      descriptions = names_shapes.collect { |s| shapes[s][:description] }
      description = descriptions[0]
      plurals = names_shapes.collect { |s| shapes[s][:plural] }
      plural = plurals[0]
      snake_case_name = Solis::Utils::String.camel_to_snake(Solis::Utils::String.extract_name_from_uri(name_entity))
      namespace_entity = Solis::Utils::String.extract_namespace_from_uri(name_entity)
      prefix_entity = context_inv[namespace_entity]
      entities[name_entity] = {
        direct_parents: Shapes.get_parent_classes_for_class(shapes, name_entity),
        all_parents: Shapes.get_all_parent_classes_for_class(shapes, name_entity),
        properties: get_properties_info_for_entity(shapes, name_entity),
        own_properties: get_own_properties_list_for_entity(shapes, name_entity),
        name: name,
        prefix: prefix_entity,
        description: description,
        plural: plural,
        snake_case_name: snake_case_name
      }
    end

    data = {
      namespace: graph_namespace,
      title: graph_title,
      version: graph_version,
      version_counter: graph_version_counter,
      description: graph_description,
      entities: entities
    }

    return data if raw
    data.to_json
  end

  private

  def self.deep_copy(obj)
    Marshal.load(Marshal.dump(obj))
  end

  def self.get_properties_info_for_entity(shapes, name_entity)
    properties = {}
    names_shapes = Shapes.get_shapes_for_class(shapes, name_entity)
    names_shapes.each do |name_shape|
      property_shapes = deep_copy(shapes[name_shape][:properties])
      merge_info_entity_properties!(properties, property_shapes_as_entity_properties(property_shapes))
    end
    names_entities_parents = Shapes.get_all_parent_classes_for_class(shapes, name_entity)
    names_entities_parents.each do |name_entity_parent|
      names_shapes_parent = Shapes.get_shapes_for_class(shapes, name_entity_parent)
      names_shapes_parent.each do |name_shape_parent|
        property_shapes_parent = deep_copy(shapes[name_shape_parent][:properties])
        properties_parent = property_shapes_as_entity_properties(property_shapes_parent)
        merge_info_entity_properties!(properties, properties_parent)
      end
    end
    properties
  end

  def self.property_shapes_as_entity_properties(property_shapes)
    properties = {}
    property_shapes.each_value do |shape|
      next if Shapes.is_property_shape_for_list_container?(shape)
      name_property = Shapes.extract_property_name_from_path(shape[:path])
      unless properties.key?(name_property)
        properties[name_property] = { constraints: [] }
      end
      constraints = deep_copy(shape[:constraints])
      if constraints.key?(:or)
        constraints[:or].map! do |o|
          h = {
            o[:path] => o
          }
          property_shapes_as_entity_properties(h).values[0]
        end
      end
      properties[name_property][:constraints] << {
        description: shape[:description],
        data: constraints
      }
    end
    property_shapes.each_value do |shape|
      next unless Shapes.is_property_shape_for_list_container?(shape)
      name_property = Shapes.extract_property_name_from_path(shape[:path])
      unless properties.key?(name_property)
        properties[name_property] = {
          constraints: [
            {
              data: {}
            }
          ]
        }
      end
      properties[name_property][:constraints][0][:data][:sorted] = true
    end
    properties
  end

  def self.get_own_properties_list_for_entity(shapes, name_entity)
    list_properties = []
    names_shapes = Shapes.get_shapes_for_class(shapes, name_entity)
    names_shapes.each do |name_shape|
      property_shapes = deep_copy(shapes[name_shape][:properties])
      list_properties.concat(property_shapes.values.map { |v| Shapes.extract_property_name_from_path(v[:path]) })
    end
    list_properties.uniq
  end

  def self.merge_info_entity_properties!(properties_1, properties_2)
    properties_2.each do |k, v|
      if properties_1.key?(k)
        properties_1[k][:constraints].concat(v[:constraints])
      else
        properties_1[k] = v
      end
    end
  end

end