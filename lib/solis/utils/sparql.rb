
require 'sparql'

module SPARQL
  class Client

    def query(query, **options)
      @op = :query
      @alt_endpoint = options[:endpoint]
      case @url
      when RDF::Queryable
        require 'sparql' unless defined?(::SPARQL::Grammar)
        begin
          SPARQL.execute(query, @url, optimize: true, **options)
        rescue SPARQL::MalformedQuery
          $stderr.puts "error running #{query}: #{$!}"
          raise
        end
      else
        # The following is the only part changed from original source.
        # Useful for debugging reponse.body issues.
        response = response(query, **options)
        # response.body.gsub!('rdf:type', '<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>')
        parse_response(response, **options)
      end
    end

    def parse_report(response)
      # NOTE: all below tested on Virtuoso open-source only
      # following fixes a response content bug
      response.body.gsub!('rdf:type', '<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>')
      parsed = parse_response(response)
      report = {}
      if parsed.is_a?(RDF::NTriples::Reader)
        graph = RDF::Graph.new
        graph << parsed
        str_report = graph.query([nil, RDF::URI('http://www.w3.org/2005/sparql-results#value'), nil]).first_object.to_s
        report = parse_str_report(str_report)
      elsif parsed.is_a?(Array)
        str_report = parsed[0]["callret-0"].to_s
        report = parse_str_report(str_report)
      end
      report
    end

    private

    def parse_str_report(str_report)
      report = []
      str_ops = str_report.split("\n")
      str_ops.each do |str_op|
        next if str_op.include?('Commit')
        op = {}
        if str_op.start_with?('Delete')
          op[:count_delete] = str_op.scan(/[0-9]+/)[0].to_i
          op[:count_update] = op[:count_delete]
        elsif str_op.start_with?('Insert')
          op[:count_insert] = str_op.scan(/[0-9]+/)[0].to_i
          op[:count_update] = op[:count_insert]
        elsif str_op.start_with?('Modify')
          op[:count_delete], op[:count_insert] = str_op.scan(/[0-9]+/).collect { |v| v.to_i }
          op[:count_update] = op[:count_delete] + op[:count_insert]
        end
        report << op
      end
      report
    end

  end
end
