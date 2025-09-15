require_relative 'rdf_proxy'

module Solis
  class Store
    class Memory < RDFProxy
      def initialize(name_graph="http://example.com/", params = {})
        repository = RDF::Repository.new
        super(repository, name_graph, params)
      end
    end
  end
end