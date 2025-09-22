
# NOTE on SPARQL Update (https://www.w3.org/TR/sparql11-update/)
#
# Important parts:
#
# 2.2 SPARQL 1.1 Update Services
# SPARQL 1.1 Update requests are sequences of operations.
#
# Each request SHOULD be treated atomically by a SPARQL 1.1 Update service.
# The term 'atomically' means that a single request will result in either
# no effect or a complete effect, # regardless of the number of operations
# that may be present in the request.
# Any resulting concurrency issues will be a matter for each implementation
# to consider according to its own architecture. # In particular, using the
# SERVICE keyword in the WHERE clause of operations in an Update request will
# usually result in a loss of atomicity.

# 3 SPARQL 1.1 Update Language
# SPARQL 1.1 Update supports two categories of update operations on a Graph Store:
#
# A request is a sequence of operations and is terminated by EOF (End of File).
# Multiple operations are separated by a ';' (semicolon) character. A semicolon
# after the last operation in a request is optional. Implementations MUST ensure
# that the operations of a single request are executed in a fashion that guarantees
# the same effects as executing them sequentially in the order they appear in the request.
#
# Operations all result either in success or failure. A failure result MAY be accompanied
# by extra information, indicating that some portion of the operations in the request were
# successful. This document does not stipulate the exact form of the result, as that will
# be dependent on the interface being used, for instance the SPARQL 1.1 protocol via HTTP
# or a programmatic API. If multiple operations are present in a single request, then a
# result of failure from any operation MUST abort the sequence of operations, causing the
# subsequent operations to be ignored.
#
# ====================================================================================
#
# There seems to be no certainty on atomicity of either requests or operations.
# Hence, it is better to squash updates in fewer queries as possible, to mitigate
# possible non-atomicity. This is the approach that will be followed.
# If the triple store query planner is not smart enough, this also can have benefits
# on performance.

require 'sparql'
require 'securerandom'

require_relative 'common'


module Solis
  class Store

    module RDFOperationsRunner

      # Expects:
      # - @client_sparql
      # - @logger
      # - @mutex_repository

      def query_langs
        ['SPARQL']
      end

      def run_operations(ids_op='all')
        ops_read = []
        ops_write = []
        ops_any = []
        indexes = []
        ids_op = [ids_op] if (ids_op.is_a?(String) && !ids_op.eql?('all'))
        @ops.each_with_index do |op, index|
          if ids_op.is_a?(Array)
            next unless ids_op.include?(op['id'])
          end
          if op['type'].eql?('read')
            ops_read << op
          elsif op['type'].eql?('write')
            ops_write << op
          else
            ops_any << op
          end
          indexes << index
        end
        res = {}
        # There must be guaranteed that in the following functions call
        # all exceptions are handled internally, and return in the results.
        # This way, @ops can be updated successfully.
        res.merge!(run_write_operations(ops_write))
        res.merge!(run_read_operations(ops_read))
        res.merge!(run_any_operations(ops_any))
        # remove performed operations from list;
        # following does not seem thread-safe, but ok for the now ...
        indexes.sort.reverse_each { |index| @ops.delete_at(index) }
        res
      end

      private

      def init_db
        @client_sparql.insert_data(RDF::Graph.new { |graph|
          graph << [
            RDF::URI('https://example.com/dummy_s'),
            RDF::URI('https://example.com/dummy_p'),
            RDF::URI('https://example.com/dummy_o')
          ]
        })
      end

      def parse_json_value_from_datatype(str_value, datatype)
        case datatype
        when /http:\/\/www.w3.org\/2001\/XMLSchema#integer/,
          /http:\/\/www.w3.org\/2001\/XMLSchema#int/
          v = str_value.to_i
        when /http:\/\/www.w3.org\/2001\/XMLSchema#boolean/
          v = str_value == "true"
        when /http:\/\/www.w3.org\/2001\/XMLSchema#float/,
          /http:\/\/www.w3.org\/2001\/XMLSchema#double/
          v = str_value.to_f
        else
          v = str_value
        end
        v
      end

      def get_data_for_subject(s, context, deep)
        # create graph of query results
        graph = RDF::Graph.new
        traversed = []
        fill_graph_from_subject_root = lambda do |g, s, traversed, deep|
          return if traversed.include?(s.to_s)
          traversed << s.to_s
          query = @client_sparql.select.where([s, :p, :o])
          query.each_solution do |solution|
            @logger.debug([s, solution.p, solution.o])
            g << [s, solution.p, solution.o]
            if solution.o.node?
              list = get_list_object_for_subject_and_predicate(s, solution.p, o: solution.o)
              g << list
            end
            if deep
              # if solution.o.is_a?(RDF::URI) or solution.o.is_a?(RDF::Literal::AnyURI)
              if solution.o.is_a?(RDF::URI)
                # unless traversed.include?(solution.o.to_s)
                  fill_graph_from_subject_root.call(g, RDF::URI(solution.o), traversed, deep)
                # end
              end
            end
          end
        end
        fill_graph_from_subject_root.call(graph, s, traversed, deep)
        # turn graph into JSON-LD hash
        jsonld = JSON::LD::API.fromRDF(graph)
        @logger.debug(JSON.pretty_generate(jsonld))
        # compact @type
        jsonld_compacted = jsonld.map do |obj|
          Solis::Utils::JSONLD.compact_type(obj)
        end
        # adjust some sub-fields;
        # turning {"@type": "http://www.w3.org/2001/XMLSchema#anyURI", "@value": "<uri>"}
        # into {"@id": "<uri>"} is necessary for the correct framing just later;
        # having a reference in format
        # {"@type": "http://www.w3.org/2001/XMLSchema#anyURI", "@value": "<uri>"}
        # is caused by it in case it is stored as literal with datatype
        # "http://www.w3.org/2001/XMLSchema#anyURI"
        # jsonld_compacted.map! do |obj|
        #   Solis::Utils::JSONLD.anyuris_to_uris(obj)
        # end
        @logger.debug(JSON.pretty_generate(jsonld_compacted))
        f_conv = method(:parse_json_value_from_datatype)
        # compact also the values
        jsonld_compacted.map! do |obj|
          Solis::Utils::JSONLD.compact_values(obj, f_conv)
        end
        @logger.debug(JSON.pretty_generate(jsonld_compacted))
        # find the type of the (root) object with URI "s"
        obj_root = jsonld_compacted.find { |e| e['@id'] == s.to_s }
        type = obj_root.nil? ? nil : obj_root['@type'][0]
        # frame JSON-LD; this will:
        # - compact attributes thanks to "@vocab"
        # - embed (at any depth) objects to the root one, thanks to @embed;
        # this needs references in {"@id": "<uri>"} format (see above)
        # - avoid having other objects but the root one, thanks to "@type" filter
        frame = JSON.parse %(
          {
            "@context": #{context.to_json},
            "@type": "#{type}",
            "@embed": "@always"
          }
        )
        jsonld_compacted_framed = JSON::LD::API.frame(jsonld_compacted, frame)
        @logger.debug(JSON.pretty_generate(jsonld_compacted_framed))
        # produce result
        res = {}
        message = ""
        success = true
        # if framing created a "@graph" (empty) attribute,
        # then there was either no matching result in the framing,
        # or embedded objects with the same type (only first matters)
        if jsonld_compacted_framed.key?('@graph')
          if jsonld_compacted_framed['@graph'].size == 0
            message = "no entity with id '#{s.to_s}'"
            success = false
          else
            res = jsonld_compacted_framed['@graph'][0]
            res['@context'] = jsonld_compacted_framed['@context']
          end
        else
          res = jsonld_compacted_framed
        end
        context = res.delete('@context')
        {
          "success" => success,
          "message" => message,
          "data" => {
            "obj" => res,
            "context" => context
          }
        }
      end

      def ask_if_object_is_referenced(o)
        # to make this more robust, the for object that are:
        # - URI (within triangular braces, like <uri>)
        # - a literal of type http://www.w3.org/2001/XMLSchema#anyURI
        result = @client_sparql.ask.whether([:s, :p, o]).true?
        o_literal = RDF::Literal.new(o.to_s, datatype: 'http://www.w3.org/2001/XMLSchema#anyURI')
        result_literal = @client_sparql.ask.whether([:s, :p, o_literal]).true?
        result or result_literal
      end

      def ask_if_subject_exists(s)
        result = @client_sparql.ask.whether([s, :p, :o]).true?
        result
      end

      def run_read_operations(ops_generic)
        res = ops_generic.map do |op|
          case op['name']
          when 'get_data_for_id'
            id = op['content'][0]
            context = op['content'][1]
            deep = op['opts'] == Solis::Store::GetMode::DEEP
            s = RDF::URI(id)
            r = get_data_for_subject(s, context, deep)
          when 'ask_if_id_is_referenced'
            id = op['content'][0]
            o = RDF::URI(id)
            r = ask_if_object_is_referenced(o)
          when 'ask_if_id_exists'
            id = op['content'][0]
            s = RDF::URI(id)
            r = ask_if_subject_exists(s)
          end
          [op['id'], r]
        end.to_h
        res
      end

      def run_any_operations(ops_generic)
        res = ops_generic.map do |op|
          case op['name']
          when 'run_raw_query'
            query, type_query = op['content']
            r = _run_raw_query(query, type_query)
          end
          [op['id'], r]
        end.to_h
        res
      end


      def _run_raw_query(query, type_query)
        results = @client_sparql.query(query)
        case type_query
        when 'find_records'
          results.map { |solution| solution[:s].to_s }
        when 'count_records'
          results.first[:count].to_i
        end
      end

      def run_write_operations(ops)
        ops_save = []
        ops_destroy = []
        ops.each do |op|
          if op['name'].eql?('delete_attributes_for_id')
            ops_destroy << op
          elsif op['name'].eql?('delete_all')
            ops_destroy << op
          else
            ops_save << op
          end
        end
        res = {}
        res.merge!(run_save_operations(ops_save))
        res.merge!(run_destroy_operations(ops_destroy))
        res
      end

      def run_destroy_operations(ops)
        ops_destroy_for_id = []
        ops_destroy_all = []
        ops.each do |op|
          if op['name'].eql?('delete_attributes_for_id')
            ops_destroy_for_id << op
          else
            ops_destroy_all << op
          end
        end
        res = {}
        res.merge!(run_destroy_for_id_operations(ops_destroy_for_id))
        res.merge!(run_destroy_all_operations(ops_destroy_all))
        res
      end

      def run_destroy_for_id_operations(ops_generic)
        ss = []
        ops_generic.each do |op|
          id = op['content'][0]
          s = RDF::URI(id)
          ss << s
        end
        r = delete_attributes_for_subjects(ss)
        res = {}
        ops_generic.each do |op|
          res[op['id']] = r
        end
        res
      end

      def run_destroy_all_operations(ops_generic)
        res = {}
        ops_generic.each do |op|
          res[op['id']] = _delete_all
        end
        res
      end

      def _delete_all
        report = {}
        case @client_sparql.url
        when RDF::Queryable
          repository = @client_sparql.url
          if @mutex_repository.nil?
            report[:count_delete] = repository.count
            report[:count_update] = report[:count_delete]
            repository.clear!
          else
            @mutex_repository.synchronize do
              report[:count_delete] = repository.count
              report[:count_update] = report[:count_delete]
              repository.clear!
            end
          end
        else
          # NOTE: this better be a DELETE DATA.
          # DELETE/WHERE needs a pattern match to delete all data.
          # On Virtuoso, it fails when no triples exist.
          query = "
            WITH GRAPH <#{@name_graph}>
            DELETE {
                ?s ?p ?o .
            } WHERE {
                ?s ?p ?o .
            }
          "
          response = @client_sparql.response(query)
          report = @client_sparql.parse_report(response)
        end
        init_db
        {
          "success" => true,
          "message" => report
        }
      end

      def delete_attributes_for_subjects(ss)
        unless ss.empty?
          str_ids = ss.map { |s| "<#{s.to_s}>" } .join(' ')
          # This query string takes care of:
          # - deleting attributes of one of more subjects
          # - checking that those subjects are not objects in other triples
          # (i.e. they are not referenced)
          # Both together in the same query.
          str_query = %(
                    DELETE {
                      ?s ?p ?o
                    }
                    WHERE {
                      FILTER NOT EXISTS { ?s_ref ?p_ref ?s } .
                      VALUES ?s { #{str_ids} } .
                      ?s ?p ?o .
                    }
                  )
          @logger.debug("\n\nDELETE QUERY:\n\n")
          @logger.debug(str_query)
          @logger.debug("\n\n")

          success = true
          err_code = 0
          message = ""
          message_save_counters_not_available = "save counters not available (Virtuoso-only supported)"
          message_subjects_were_referenced = "any of these '#{str_ids}' was referenced"
          message_subjects_were_referenced_or_not_existing = "any of these '#{str_ids}' was referenced or not existing"

          case @client_sparql.url
          when RDF::Queryable

            perform_delete_report_atomic = lambda do
              @client_sparql.query(str_query, update: true)
              subjects_were_referenced = @client_sparql.ask
                                                       .where([:s_ref, :p_ref, :s])
                                                       .values(:s, *ss)
                                                       .true?
              report = { subjects_were_referenced: subjects_were_referenced }
              report
            end
            report = nil
            if @mutex_repository.nil?
              report = perform_delete_report_atomic.call
            else
              @mutex_repository.synchronize do
                report = perform_delete_report_atomic.call
              end
            end

            if report[:subjects_were_referenced]
              success = false
              err_code = 2
              message = message_subjects_were_referenced
            end

          else

            response = @client_sparql.response(str_query)
            report = @client_sparql.parse_report(response)
            if report.collect {|e| e[:count_update]} .include?(nil)
              success = false
              err_code = 1
              message = message_save_counters_not_available
            end
            if report.collect {|e| e[:count_update]} .include?(0)
              success = false
              err_code = 2
              message = message_subjects_were_referenced_or_not_existing
            end

          end

          {
            "success" => success,
            "err_code" => err_code,
            "message" => message
          }
        end
      end

      def run_save_operations(ops_generic)

        return {} if ops_generic.empty?

        # convert endpoint-agnostic operations into RDF operations
        ops = ops_generic.map do |op|
          op_rdf = Marshal.load(Marshal.dump(op))
          case op['name']
          when 'save_id_with_type'
            id, _, type = op_rdf['content']
            s, p, o = [RDF::URI(id), RDF::RDFV.type, RDF::URI(type)]
            op_rdf['content'] = [s, p, o]
          when 'save_attribute_for_id'
            id, name_attr, val_attr, type_attr = op_rdf['content']
            s, p, o = prepare_statement(id, name_attr, val_attr, type_attr)
            op_rdf['content'] = [s, p, o]
          when 'delete_attribute_for_id'
            id, name_attr = op_rdf['content']
            s, p = prepare_subject_and_predicate(id, name_attr)
            op_rdf['content'] = [s, p]
          else
            op_rdf = nil
          end
          op_rdf
        end.compact

        ops_filters = ops_generic.map do |op|
          op_rdf = Marshal.load(Marshal.dump(op))
          case op['name']
          when 'set_attribute_condition_for_saves'
            id, name_attr, val_attr, type_attr = op_rdf['content']
            s, p, o = prepare_statement(id, name_attr, val_attr, type_attr)
            op_rdf['row_where'] = RDF::Statement(s, p, o).to_s
          when 'set_not_existing_id_condition_for_saves'
            id = op_rdf['content'][0]
            op_rdf['row_where'] = "FILTER NOT EXISTS { <#{id}> ?b ?c }"
          else
            op_rdf = nil
          end
          op_rdf
        end.compact

        clause_where = ops_filters.map { |op| op['row_where'] } .join("\n")
        clause_where = "?s ?p ?o .\n#{clause_where}"

        # NOTE:
        # For 'set_not_existing_id_condition_for_saves', for RDF::Repository,
        # the following slightly simplified where clause also works:
        #
        #   FILTER NOT EXISTS { <uri-1> ?p ?o }
        #   FILTER NOT EXISTS { <uri-2> ?p ?o }
        #   ...
        #   FILTER NOT EXISTS { <uri-N> ?p ?o }
        #
        # For Virtuoso, or perhaps all other triple stores (?), this does not work.
        # The working query is:
        #
        #   ?s ?p ?p .
        #   FILTER NOT EXISTS { <uri-1> ?b ?c }
        #   FILTER NOT EXISTS { <uri-2> ?b ?c }
        #   ...
        #   FILTER NOT EXISTS { <uri-N> ?b ?c }
        #
        # This is important to read: https://stackoverflow.com/questions/63664058/the-mechanism-of-filter-not-exists-in-sparql
        # To work properly in Virtuoso, there seems to be th need of having "?s ?p ?p ." where clause.
        # This add the extra needs to always have at least one triple in the graph, for the above to work correctly.
        # Other SHACL playground, like https://atomgraph.github.io/SPARQL-Playground/, seems to work correctly.

        # create empty delete graph
        insert = {
          'graph' => RDF::Graph.new
        }

        clause_delete = ""

        # create an operations cache:
        # group operations by subject and predicate.
        # it can be useful later.
        cache_ops = {}
        ops.each do |op|
          st = op['content']
          key_sp = "#{st[0].to_s}_#{st[1].to_s}"
          cache_ops[key_sp] = [] unless cache_ops.key?(key_sp)
          cache_ops[key_sp] << st[2]
        end

        # write graphs
        ops.each do |op|

          case op['opts']

          when Solis::Store::SaveMode::PRE_DELETE_PEERS
            st = op['content']
            objects = get_objects_for_subject_and_predicate(st[0], st[1])
            if objects.empty?
              # attribute is not present; add it
              add_statement_to_graph(insert['graph'], st)
            else
              # pre-delete peer attributes, don't care about what exists;
              objects.each do |o|
                tmp = make_delete_where_query_parts_from_statement([st[0], st[1], o])
                clause_delete += tmp[0]
                clause_where += tmp[1]
              end
              # add new attribute values
              add_statement_to_graph(insert['graph'], st)
            end

          when Solis::Store::SaveMode::PRE_DELETE_PEERS_IF_DIFF_SET
            st = op['content']
            objects = get_objects_for_subject_and_predicate(st[0], st[1])
            if objects.empty?
              # attribute is not present; add it
              add_statement_to_graph(insert['graph'], st)
            else
              key_sp = "#{st[0].to_s}_#{st[1].to_s}"
              if objects.sort != cache_ops[key_sp].sort
                # attribute is present but with different values that the ones to write;
                # stage those old ones for deletion
                objects.each do |o|
                  tmp = make_delete_where_query_parts_from_statement([st[0], st[1], o])
                  clause_delete += tmp[0]
                  clause_where += tmp[1]
                end
                # add new attribute values
                add_statement_to_graph(insert['graph'], st)
              end
            end

          when Solis::Store::SaveMode::APPEND_IF_NOT_PRESENT
            st = op['content']
            objects = get_objects_for_subject_and_predicate(st[0], st[1])
            if objects.empty?
              # attribute is not present; add it
              add_statement_to_graph(insert['graph'], st)
            else
              unless objects.include?(st[2])
                # peer attributes exist, but not with this value;
                # stage those old ones for deletion
                objects.each do |o|
                  tmp = make_delete_where_query_parts_from_statement([st[0], st[1], o])
                  clause_delete += tmp[0]
                  clause_where += tmp[1]
                end
                # add new attribute values
                add_statement_to_graph(insert['graph'], st)
              end
            end

          when Solis::Store::DeleteMode::DELETE_ATTRIBUTE
            st = op['content']
            objects = get_objects_for_subject_and_predicate(st[0], st[1])
            # delete peer attributes;
            objects.each do |o|
              tmp = make_delete_where_query_parts_from_statement([st[0], st[1], o])
              clause_delete += tmp[0]
              clause_where += tmp[1]
            end

          end

        end

        nothing_to_save = false

        success = true
        err_code = 0
        message = ""
        message_dirty = "data is dirty"
        message_save_counters_not_available = "save counters not available (Virtuoso-only supported)"

        unless nothing_to_save

          method = 2

          case method
          when 1

          when 2

            method_di = 3

            case method_di

            when 3

              case @client_sparql.url
              when RDF::Queryable

                perform_delete_insert_where_with_report_atomic = lambda do
                  str_query_ask = "ASK WHERE {\n#{clause_where} }"
                  @logger.debug("\n\nASK QUERY:\n\n")
                  @logger.debug(str_query_ask)
                  @logger.debug("\n\n")
                  has_pattern = @client_sparql.query(str_query_ask).true?
                  @logger.debug("has_pattern: #{has_pattern}")
                  if has_pattern
                    str_queries = create_delete_insert_where_query(clause_delete, insert['graph'], clause_where, name_graph=nil, split_queries=true)
                    str_queries.each_with_index do |str_query, i|
                      @logger.debug("\n\nDELETE/INSERT QUERY #{i+1}:\n\n")
                      @logger.debug(str_query)
                      @logger.debug("\n\n")
                      @client_sparql.update(str_query)
                    end
                  end
                  report = { can_update: has_pattern }
                  report
                end
                report = nil
                if @mutex_repository.nil?
                  report = perform_delete_insert_where_with_report_atomic.call
                else
                  @mutex_repository.synchronize do
                    report = perform_delete_insert_where_with_report_atomic.call
                  end
                end
                unless report[:can_update]
                  success = false
                  err_code = 1
                  message = message_dirty
                end


              else

                str_query_ask = "ASK WHERE { GRAPH <#{@name_graph}> {\n#{clause_where}} }"

                # "has_pattern" not used, just to check pattern match in single-thread debugging
                @logger.debug("\n\nASK QUERY:\n\n")
                @logger.debug(str_query_ask)
                @logger.debug("\n\n")
                has_pattern = @client_sparql.query(str_query_ask)
                @logger.debug("has_pattern: #{has_pattern}")

                query = create_delete_insert_where_query(clause_delete, insert['graph'], clause_where, name_graph=@name_graph)
                @logger.debug("\n\nDELETE/INSERT QUERY:\n\n")
                @logger.debug(query)
                @logger.debug("\n\n")
                response = @client_sparql.response(query)
                report = @client_sparql.parse_report(response)
                if report.collect {|e| e[:count_update]} .include?(nil)
                  success = false
                  err_code = 1
                  message = message_save_counters_not_available
                end
                if report.collect {|e| e[:count_update]} .include?(0)
                  success = false
                  err_code = 2
                  message = message_dirty
                end

              end

            end

          end

        end

        res = ops_generic.map do |op|
          [op['id'], {
            "success" => success,
            "message" => message,
            "err_code" => err_code
          }]
        end.to_h
        res

      end

      def prepare_subject(id)
        RDF::URI(id)
      end

      def prepare_subject_and_predicate(id, name_attr)
        s = RDF::URI(id)
        p = RDF::URI(name_attr)
        [s, p]
      end

      def prepare_object(val_attr, type_attr)
        if type_attr.eql?('URI')
          o = RDF::URI(val_attr)
        elsif type_attr.eql?('list')
          o = RDF::List.new
          val_attr.each do |v|
            o << prepare_object(v[0], v[1])
          end
        else
          type_attr_known = RDF::Vocabulary.find_term(type_attr)
          type_attr = type_attr_known unless type_attr_known.nil?
          o = RDF::Literal.new(val_attr, datatype: type_attr)
        end
        o
      end

      def prepare_statement(id, name_attr, val_attr, type_attr)
        s, p = prepare_subject_and_predicate(id, name_attr)
        o = prepare_object(val_attr, type_attr)
        [s, p, o]
      end

      def add_statement_to_graph(graph, st)
        if st[2].is_a?(RDF::List)
          graph << st[2]
          graph << [st[0], st[1], st[2].subject]
        else
          graph << st
        end
      end

      def make_delete_where_query_parts_from_statement(st)
        # see: https://seaborne.blogspot.com/2011/03/updating-rdf-lists-with-sparql.html
        delete_clause = ""
        where_clause = ""
        if st[2].is_a?(RDF::List)
          id_var = SecureRandom.hex
          list = "list_#{id_var}"
          z = "z_#{id_var}"
          head = "head_#{id_var}"
          tail = "tail_#{id_var}"
          delete_clause += "
            ?#{z} <#{RDF::RDFV.first}> ?#{head} .
            ?#{z} <#{RDF::RDFV.rest}> ?#{tail} .
            #{st[0].to_nquads} #{st[1].to_nquads} ?#{z} .
          "
          where_clause += "
            #{st[0].to_nquads} #{st[1].to_nquads} ?#{list} .
            ?#{list} <#{RDF::RDFV.rest}>* ?#{z} .
            ?#{z} <#{RDF::RDFV.first}> ?#{head} .
            ?#{z} <#{RDF::RDFV.rest}> ?#{tail} .
          "
        else
          delete_clause += "
            #{st[0].to_nquads} #{st[1].to_nquads} #{st[2].to_nquads} .
          "
        end
        [delete_clause, where_clause]
      end

      def get_objects_for_subject_and_predicate(s, p)
        objects = []
        result = @client_sparql.select.where([s, p, :o])
        result.each_solution do |solution|
          if solution.o.nil? || solution.o.node?
            # it is a blank node starting a list
            objects << get_list_object_for_subject_and_predicate(s, p)
          else
            objects << solution.o
          end
        end
        @logger.debug("GET_OBJECTS_FOR_SUBJECT_AND_PREDICATE: #{s}, #{p}:\n#{objects}")
        objects
      end

      def get_list_object_for_subject_and_predicate(s, p, o: nil)
        result = @client_sparql.query("
          SELECT ?item
          WHERE {
            <#{s.to_s}> <#{p.to_s}>/<#{RDF.rest}>*/<#{RDF.first}> ?item
          }
        ")
        items = []
        result.each_solution do |solution|
          items << solution.item
        end
        # NOTE: "subject" is ignored if "values" is empty
        list = RDF::List.new(subject: o, values: items)
        list
      end

      def create_delete_insert_where_query(clause_delete, graph_insert, clause_where, name_graph=nil, split_queries=false)
        # rdf:List present some challenges in a DELETE/INSERT/WHERE query: it has blank nodes.
        # For the INSERT part, this hqs drawbacks, see here (from https://www.w3.org/TR/sparql11-update/):
        #
        # "Blank nodes that appear in an INSERT clause operate similarly to blank nodes in the template
        # of a CONSTRUCT query, i.e., they are re-instantiated for any solution of the WHERE clause;"
        #
        # This fact has been proven experimentally on Virtuoso.
        # Other SPARQL engines deviate from SPARQL standard (e.g. https://gnome.pages.gitlab.gnome.org/tinysparql/tutorial.html#blank-nodes),
        # but Virtuoso does not.
        # Hence, a way to insert the list content only *once*.
        # The following idea is applied:
        # split the insert data content in 2 DELETE/INSERT/WHERE queries:
        # 1) The first one takes case of handling all triples without blank nodes, as usual.
        # If triples with blank nodes exist, it will also add a *single* fictitious extra triple.
        # This will be used in the next query.
        # 2) The second query handles only triples with blank nodes (e.g. rdf:List);
        # in its WHERE and DELETE clauses, only the single fictitious triple is used, while in the INSERT clause
        # the triples with blank nodes are included.
        # The 2 queries are now locked:
        # When first query will insert, then the second query can insert (this time only once) as well.
        # When first query will not insert, then the second query cannot insert as well.
        graph_insert_std = RDF::Graph.new
        graph_insert_bnodes = RDF::Graph.new
        graph_where_bnodes = RDF::Graph.new
        graph_insert.each do |statement|
          if statement.subject.node? || statement.object.node?
            graph_insert_bnodes << statement
          else
            graph_insert_std << statement
          end
        end
        str_query_1 = ""
        str_query_2 = ""
        str_query = ""
        unless name_graph.nil?
          str_query += "\nWITH <#{name_graph}>"
        end
        if graph_insert_bnodes.empty?
          str_query += "\nDELETE { \n#{clause_delete} } INSERT { \n#{graph_insert.dump(:ntriples)} } WHERE { \n#{clause_where} \n}"
        else
          id_lock = "lock_#{SecureRandom.hex}"
          statement_lock = [RDF::URI("#{@name_graph}#{id_lock}"), RDF::URI("#{@name_graph}locked"), true]
          graph_where_bnodes << statement_lock
          graph_insert_std << statement_lock
          str_query_1 += "\nDELETE { \n#{clause_delete} } INSERT { \n#{graph_insert_std.dump(:ntriples)} } WHERE { \n#{clause_where} \n};"
          str_query += str_query_1
          str_query_2 += "\nDELETE { \n#{graph_where_bnodes.dump(:ntriples)} } INSERT { \n#{graph_insert_bnodes.dump(:ntriples)} } WHERE { \n#{graph_where_bnodes.dump(:ntriples)} \n}"
          str_query += str_query_2
        end
        res = str_query
        if split_queries
          if graph_insert_bnodes.empty?
            res = [str_query]
          else
            res = [str_query_1, str_query_2]
          end
        end
        res
      end

    end

  end
end