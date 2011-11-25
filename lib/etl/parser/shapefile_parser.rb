require 'yajl/json_gem'

module ETL #:nodoc:
  module ETL::Parser #:nodoc:
    # Parses ESRI Shapefiles
    class ShapefileParser < ETL::Parser::Parser
      # Initialize the parser
      # * <tt>source</tt>: The Source object
      # * <tt>options</tt>: Hash of options for the parser, defaults to an empty hash
      def initialize(source, options={})
        super
        configure
      end
      
      def get_fields_names(file)
        File.open(file) do |input|
          fields = CSV.parse(input.readline, options).first
          new_fields = []
          fields.each_with_index do |field,index|
            # compute the index of occurrence of this specific occurrence of the field (usually, will be 1)
            occurrence_index = fields[0..index].find_all { |e| e == field }.size
            number_of_occurrences = fields.find_all { |e| e == field }.size
            new_field = field + (number_of_occurrences > 1 ? "_#{occurrence_index}" : "")
            new_fields << Field.new(new_field.to_sym)
          end
          return new_fields
        end
      end

      # Returns each row.
      def each
        Dir.glob(file).each do |file|
          ETL::Engine.logger.debug "parsing #{file}"

          geo_file = shapefile_to_json(file)
          
          feature_collection = JSON.parse(geo_file)
          feature_collection["features"].each {|row| yield row}
        end
      end
      
      # Uses ogr2ogr to transform ESRI Shapefile to GeoJSON
      def shapefile_to_json(file)

        # Ask OS for ogr2ogr executable location
        ogr2ogr = %x[which ogr2ogr].strip
        raise "Shapefile parser requires GDAL ogr2ogr application." if ogr2ogr.blank?

        sub_dir = Digest::MD5.hexdigest(Time.now.to_i.to_s + rand.to_s).to_s
        temp_dir = File.join(Dir.tmpdir, sub_dir)
        
        # Unzip file into temporary dir
        `#{UNZIP} #{file} -d #{temp_dir}`
      
        shapefile = Dir.glob(File.join(temp_dir, '*.shp')).first
        raise "No .shp file found inside zip" unless !shapefile.nil? && File.exists?(shapefile)

        geojson_file = File.join(temp_dir, shapefile.gsub(/\.shp/,'.js'))
        
        # Execute ogr2ogr to convert from shapefile to geojson format
        %x!#{ogr2ogr} -t_srs #{@output_projection} -a_srs #{@output_projection} -f "GeoJSON" #{geojson_file} #{shapefile}!
        raise "ogr2ogr output file #{geojson_file} not found" unless File.exists?(@geojson_file_name)
        geojson_file
      end
            
      # Get an array of defined fields
      def fields
        @fields ||= []
      end
      
      private
      def configure
        source.definition.each do |options|
          case options
          when Symbol
            fields << Field.new(options)
          when Hash
            fields << Field.new(options[:name])
          else
            raise DefinitionError, "Each field definition must either be a symbol or a hash"
          end
        end
      end
      
      class Field #:nodoc:
        attr_reader :name
        def initialize(name)
          @name = name
        end
      end
    end
  end
end