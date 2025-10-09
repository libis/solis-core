require 'http'
module Solis
  module Utils

    class PrefixResolver
      CACHE = {}

      def self.resolve_prefix(namespace)
        return CACHE[namespace] if CACHE[namespace]

        # Try multiple sources in order of preference
        prefix = try_common_vocab(namespace) || try_rdf_vocab(namespace) || try_lov(namespace) || try_prefix_cc(namespace) ||
          generate_fallback_prefix(namespace)

        CACHE[namespace] = prefix
        prefix
      end

      # def self.extract_prefixes(repository, namespace)
      def self.extract_prefixes(repository)
        prefixes = {}
        uris = []
        repository.each_statement do |statement|
          [statement.subject, statement.predicate, statement.object].each do |term|
            if term.is_a?(RDF::URI)
              # Extract potential prefix (everything before the last # or /)
              uri_str = term.to_s
              if uri_str =~ /(.*[#\/])([^#\/]+)$/
                base_uri = $1

                uris << base_uri unless uris.include?(base_uri)
              end
            end
          end
        end

        anonymous_prefix_index = 0
        uris.each do |uri|
          prefix = resolve_prefix(uri)
          if prefix.eql?('ns')
            prefixes["ns#{anonymous_prefix_index}"] = uri
            anonymous_prefix_index += 1
          else
            prefixes[prefix] = uri
          end
        end

        prefixes
      end

      private

      def self.try_lov(namespace)
          url = "https://lov.linkeddata.es/dataset/lov/api/v2/vocabulary/autocomplete"
          params = { q: namespace }

          response = HTTP.get(url, params: params)
          if response.status.success?
            data = JSON.parse(response.body)
            data.dig('results')&.first&.dig('prefix')&.first
          end
        rescue
          nil
      end

      # works best for prefix -> namespace lookups
      def self.try_prefix_cc(namespace)
        encoded_ns = CGI.escape(namespace)
        response = HTTP.get("https://prefix.cc/#{encoded_ns}.file.json")

        if response.success?
          data = JSON.parse(response.body)
          data.keys.first
        end
      rescue
        nil
      end

      def self.try_rdf_vocab(namespace)
        RDF::Vocabulary.each do |vocab|
          if vocab.to_s == namespace
            attempt = vocab.to_s.split('/').last.downcase.gsub(/\W*/,'')
            attempt_integer = Integer(attempt) rescue nil
            return attempt if attempt_integer.nil?
          end
        end
        nil
      end

      def self.try_common_vocab(namespace)
        vocab = {
          'http://www.w3.org/1999/02/22-rdf-syntax-ns#' => 'rdf',
          'http://xmlns.com/foaf/0.1/' => 'foaf',
          'http://purl.org/NET/c4dm/event.owl#' => 'event',
          'http://datashapes.org/dash#' => 'dash',
          'http://www.w3.org/2004/02/skos/core#' => 'skos',
          'http://purl.org/dc/terms/' => 'dc',
          'http://www.w3.org/2000/01/rdf-schema#' => 'rdfs',
          'http://www.w3.org/2001/XMLSchema#' => 'xsd'
        }
        vocab[namespace]
      end

      def self.generate_fallback_prefix(namespace)
        # Extract domain or generate a reasonable prefix
        uri = URI.parse(namespace)
        if uri.host
          # NOTE: "_" uglier than "-", but at least it is symbolizable
          uri.host.split('.').first + uri.path.gsub('/','_')[..-2]
        else
          'ns'
        end
      end
    end
  end
end
