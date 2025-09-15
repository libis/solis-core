

require 'sparql'
require 'logger'

require_relative 'rdf_operations_runner'
require_relative 'operations_collector'



module Solis
  class Store

    class RDFProxy

      include Solis::Store::OperationsCollector
      include Solis::Store::RDFOperationsRunner

      attr_reader :logger, :client_sparql

      def initialize(repository, name_graph, params = {})
        # all the rest:
        @logger = params[:logger] || Logger.new(STDOUT)
        @logger.level = Logger::INFO
        @repository = repository || RDF::Repository.new
        @name_graph = name_graph
        # following also for:
        # - Solis::Store::RDFOperationsRunner
        @client_sparql = SPARQL::Client.new(@repository, graph: name_graph)
        init_db
        # NOTE: the following line is used to prevent a weird bug happening
        # when interleaving usage of @client_sparql with both:
        # - query builder helpers: @client_sparql.ask/where...;
        # - @client_sparql.query(query, update: true)
        # - @client_sparql.response(query)
        # This query should be fast enough and not affect start-up time.
        # This bug is isolated and reported outside of this project.
        # It happens with:
        # - Docker image: openlink/virtuoso-opensource-7:7.2.13
        # - ruby sparql-client 3.3.0
        @client_sparql.ask.where([:s, :p, :o]).true?
        @mutex_repository = params[:mutex_repository]
        # following also for:
        # - Solis::Store::OperationsCollector
        # - Solis::Store::RDFOperationsRunner
        @ops = []
      end

      def as_repository
        repository = RDF::Repository.new
        if @repository.is_a?(RDF::Repository)
          @repository.each_statement do |statement|
            repository << statement
          end
        else
          query = @client_sparql.select.where([:s, :p, :o])
          query.each_solution do |solution|
            repository << [solution.s, solution.p, solution.o]
          end
        end
        repository.each do |statement|
          if statement.subject.to_s.start_with?('https://example.com/dummy_s')
            repository.delete(statement)
          end
        end
        repository
      end

    end

  end
end