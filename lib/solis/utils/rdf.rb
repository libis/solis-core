
require 'linkeddata'

module RDF
  class List
    def self.from_graph(graph, subject)
      parse_as_array = lambda do |arr, graph, sbj_next|
        return if sbj_next == RDF.nil
        first = graph.first_object([sbj_next, RDF.first])
        arr << first if first
        rest = graph.first_object([sbj_next, RDF.rest])
        parse_as_array.call(arr, graph, rest) if rest && rest != RDF.nil
      end
      arr = []
      parse_as_array.call(arr, graph, subject)
      RDF::List[*arr]
    end
    def self.conforms_to?(graph, subject)
      return true if subject.eql?(RDF.nil)
      return false unless subject.is_a?(RDF::Node)
      first_stmts = graph.query([subject, RDF.first, nil])
      return false if first_stmts.count != 1
      rest_stmts = graph.query([subject, RDF.rest, nil])
      return false if rest_stmts.count != 1
      rest = rest_stmts.to_a[0].object
      conforms_to?(graph, rest)
    end

    def to_expanded_a(graph)
      arr = self.to_a
      arr.map do |e|
        case e
        when RDF::URI, RDF::Literal
          e.to_s
        when RDF::Node
          e.to_expanded_o(graph)
        end
      end
    end

  end

  class Node
    def to_expanded_o(graph)
      if RDF::List.conforms_to?(graph, self)
        RDF::List.from_graph(graph, self).to_a
      else
        h = {}
        graph.query([self, nil, nil]).each do |stmt|
          h[stmt.predicate.to_s] = stmt.object.to_s
        end
        h
      end
    end
  end

end
