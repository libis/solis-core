# frozen_string_literal: true

require "test_helper"
require 'rdf/isomorphic'

class TestUtilsRDF < Minitest::Test

  def test_rdf_list_enhanced

    l = RDF::List[1, 2, 3]
    g = RDF::Graph.new << l
    l2 = RDF::List.from_graph(g, g.statements[0].subject)
    assert_equal(g.isomorphic_with?(l2.graph), true)

    assert_equal(RDF::List.conforms_to?(g, g.statements[0].subject), true)

  end

end
