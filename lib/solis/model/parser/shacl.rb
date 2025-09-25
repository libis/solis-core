require 'linkeddata'
require 'active_support/all'

class SHACLParser

  class MissingShapeNameError < StandardError
    def initialize(uri_shape, type_shape)
      msg = "#{type_shape} shape '#{uri_shape.to_s}' has no 'sh:name'"
      super(msg)
    end
  end

  class MissingPropertyShapePathError < StandardError
    def initialize(uri_shape)
      msg = "property shape '#{uri_shape.to_s}' has no 'sh:path'"
      super(msg)
    end
  end

  class DuplicateShapeNameOrURIError < StandardError
    def initialize(uri_shape, type_shape, name_or_uri)
      msg = "#{type_shape} shape with 'sh:name' or URI '#{name_or_uri}' already existing"
      super(msg)
    end
  end

  attr_reader :shapes_graph

  def initialize(shapes_graph)
    @shapes_graph = shapes_graph
  end

  def parse_shapes
    shapes = {}

    @shapes_graph.query([nil, RDF.type, RDF::Vocab::SHACL.NodeShape]) do |shape|
      shape_uri = shape.subject.to_s
      shape_name = shapes_graph.query([shape.subject, RDF::Vocab::SHACL.name, nil]).first_object.to_s
      if shape_name.empty?
        raise MissingShapeNameError.new(shape.subject, 'node')
      end
      shapes[shape_uri] = {properties: {}, uri: shape_uri, nodes: [], closed: false, plural: nil}

      @shapes_graph.query([shape.subject, RDF::Vocab::SHACL.node, nil]) do |stmt|
        node_name = stmt.object.to_s
        shapes[shape_uri][:nodes] << node_name
      end

      shapes[shape_uri][:name] = @shapes_graph.first_object([shape.subject, RDF::Vocab::SHACL.name, nil])&.to_s
      shapes[shape_uri][:target_class] = @shapes_graph.first_object([shape.subject, RDF::Vocab::SHACL.targetClass, nil])&.to_s

      shapes[shape_uri][:closed] = @shapes_graph.first_object([shape.subject, RDF::Vocab::SHACL.closed, nil])
      shapes[shape_uri][:closed] = false if shapes[shape_uri][:closed].nil?

      shapes[shape_uri][:plural] = @shapes_graph.first_object([shape.subject, RDF::Vocab::SKOS.altLabel, nil])&.to_s

      # STEP 1: Add description extraction
      shapes[shape_uri][:description] = @shapes_graph.first_object([shape.subject, RDF::Vocab::SHACL.description, nil])&.to_s

      @shapes_graph.query([shape.subject, RDF::Vocab::SHACL.property, nil]) do |property_shape|
        property_uri = property_shape.object
        property_info = extract_property_info(property_uri)

        if property_info[:path].empty?
          raise MissingPropertyShapePathError.new(shape.subject)
        end

        if property_info[:name].empty?
          raise MissingShapeNameError.new(shape.subject, 'property')
        end

        # h = shapes[shape_uri][:properties].select { |k,v| v[:name] == property_info[:name] }
        # unless h.empty?
        #   raise DuplicateShapeNameOrURIError.new(property_uri, 'property', property_info[:name])
        # end

        # prop_key = property_uri.node? ? property_info[:name] : property_uri.to_s
        prop_key = property_uri.to_s
        if shapes[shape_uri][:properties].key?(prop_key)
          raise DuplicateShapeNameOrURIError.new(property_uri, 'property', prop_key)
        end
        shapes[shape_uri][:properties][prop_key] = property_info
      end
    end

    shapes
  end

  private

  def extract_property_info(property_uri)
    property_name = shapes_graph.query([property_uri, RDF::Vocab::SHACL.name, nil]).first_object.to_s
    property_path_obj = shapes_graph.query([property_uri, RDF::Vocab::SHACL.path, nil]).first_object
    property_path = property_path_obj.to_s
    if RDF::List.conforms_to?(shapes_graph, property_path_obj)
      property_path_list = RDF::List.from_graph(shapes_graph, property_path_obj)
      property_path = property_path_list.to_expanded_a(shapes_graph)
    end
    property_info = { name: property_name, path: property_path, constraints: {} }

    shapes_graph.query([property_uri, RDF::Vocab::SHACL.description, nil]) do |max_count|
      property_info[:description] = max_count.object.to_s
    end

    shapes_graph.query([property_uri, RDF::Vocab::SHACL.datatype, nil]) do |datatype|
      property_info[:constraints][:datatype] = datatype.object.to_s
    end

    # shapes_graph.query([property_uri, RDF::Vocab::SHACL.nodeKind, nil]) do |nodeKind|
    #   property_info[:constraints][:datatype] = nodeKind.object.to_s
    # end

    shapes_graph.query([property_uri, RDF::Vocab::SHACL.pattern, nil]) do |pattern|
      property_info[:constraints][:pattern] = pattern.object.to_s
    end

    shapes_graph.query([property_uri, RDF::Vocab::SHACL.minCount, nil]) do |min_count|
      property_info[:constraints][:min_count] = min_count.object.to_i
    end

    shapes_graph.query([property_uri, RDF::Vocab::SHACL.maxCount, nil]) do |max_count|
      property_info[:constraints][:max_count] = max_count.object.to_i
    end

    shapes_graph.query([property_uri, RDF::Vocab::SHACL.class, nil]) do |klass|
      property_info[:constraints][:class] = klass.object.to_s
    end

    shapes_graph.query([property_uri, RDF::Vocab::SHACL.node, nil]) do |node|
      property_info[:constraints][:node] = node.object.to_s
    end

    shapes_graph.query([property_uri, RDF::Vocab::SHACL.or, nil]) do |or_|
      list_rdf = RDF::List.from_graph(shapes_graph, or_.object)
      property_info[:constraints][:or] = list_rdf.to_a.collect { |e| extract_property_info(e) }
    end

    property_info
  end
end

module Shapes

  def self.get_property_datatype_for_shape(shapes, name_shape, name_property)
    name_shape_property = get_property_shape_for_path(shapes, name_shape, name_property)
    shapes.dig(name_shape, :properties, name_shape_property, :constraints, :datatype)
  end

  def self.get_property_datatype_for_shape_smart(shapes, name_shape, name_property)
    name_shape_property = get_property_shape_for_path_starting_with(shapes, name_shape, name_property)
    name_shape_property = get_property_shape_for_path(shapes, name_shape, name_property) if name_shape_property.nil?
    shapes.dig(name_shape, :properties, name_shape_property, :constraints, :datatype)
  end

  def self.get_property_container_for_shape(shapes, name_shape, name_property)
    name_shape_property = get_property_shape_for_path(shapes, name_shape, name_property)
    shape_property = shapes.dig(name_shape, :properties, name_shape_property)
    res = nil
    unless shape_property.nil?
      res = 'list' if is_property_shape_for_list_container?(shape_property)
    end
    res
  end

  def self.get_property_class_for_shape(shapes, name_shape, name_property)
    name_shape_property = get_property_shape_for_path(shapes, name_shape, name_property)
    shapes.dig(name_shape, :properties, name_shape_property, :constraints, :class)
  end

  def self.get_property_class_for_shape_smart(shapes, name_shape, name_property)
    name_shape_property = get_property_shape_for_path_starting_with(shapes, name_shape, name_property)
    name_shape_property = get_property_shape_for_path(shapes, name_shape, name_property) if name_shape_property.nil?
    shapes.dig(name_shape, :properties, name_shape_property, :constraints, :class)
  end

  def self.get_parent_shapes_for_shape(shapes, name_shape)
    shapes.dig(name_shape, :nodes) || []
  end

  def self.get_target_class_for_shape(shapes, name_shape)
    shapes.dig(name_shape, :target_class)
  end

  def self.shape_exists?(shapes, name_shape)
    shapes.key?(name_shape)
  end

  def self.get_shape_for_class(shapes, name_class)
    shapes.select { |k, v| v[:target_class] == name_class }.keys.first
  end

  def self.get_shapes_for_class(shapes, name_class)
    shapes.select { |k, v| v[:target_class] == name_class }.keys
  end

  def self.find_name_by_uri(shapes, shape_uri)
    shapes[shape_uri][:name]
  end

  def self.find_uri_by_name(shapes, name)
    shapes.select{|k,v| v[:name].downcase.eql?(name.downcase)}&.keys&.first
  end

  def self.get_all_classes(shapes)
    classes = []
    shapes.each_value do |v|
      #What is target class is not defined
      next if v[:target_class].nil?
      classes << v[:target_class]
    end
    classes
  end

  def self.get_parent_classes_for_class(shapes, name_class)
    names_classes_parents = []
    names_shapes = get_shapes_for_class(shapes, name_class)
    names_shapes.each do |name_shape|
      names_nodes_parents = get_parent_shapes_for_shape(shapes, name_shape)
      names_classes_parents += names_nodes_parents.map do |uri|
        res = shapes.select { |k,v| v[:uri] == uri }
        res[uri][:target_class] rescue nil
      end.compact.uniq
    end
    names_classes_parents
  end

  def self.get_all_parent_classes_for_class(shapes, name_class)
    list = [name_class]
    idx = 0
    while true
      if idx >= list.length
        break
      end
      list += get_parent_classes_for_class(shapes, list[idx])
      idx += 1
    end
    list[1..].uniq
  end

  def self.get_property_shape_for_path(shapes, name_shape, path)
    shapes.dig(name_shape, :properties)&.select { |k, v| v[:path] == path }&.keys&.first
  end

  def self.get_property_shape_for_path_starting_with(shapes, name_shape, path)
    shapes.dig(name_shape, :properties)&.select { |k, v| v[:path].is_a?(Array) && v[:path][0] == path }&.keys&.first
  end

  def self.extract_property_name_from_path(path)
    return path[0] if is_path_for_list_items?(path)
    path
  end

  def self.is_path_for_list_items?(path)
    return false unless path.is_a?(Array)
    return false unless path.size == 3
    return false unless path[0].is_a?(String)
    return false unless path[1].is_a?(Hash)
    return false unless path[1][RDF::Vocab::SHACL.zeroOrMorePath.to_s].eql?(RDF.rest.to_s)
    return false unless path[2].eql?(RDF.first.to_s)
    true
  end

  def self.is_property_shape_for_list_container?(shape)
    shape[:constraints][:node].eql?("http://datashapes.org/dash#ListShape")
  end

end