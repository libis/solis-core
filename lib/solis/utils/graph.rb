
require 'linkeddata'
require_relative 'prefix_resolver'

module RDF
  # class Graph
  class Repository
    def extract_prefixes
      prefixes = Solis::Utils::PrefixResolver.extract_prefixes(self)
      prefixes.symbolize_keys
    end
  end
end
