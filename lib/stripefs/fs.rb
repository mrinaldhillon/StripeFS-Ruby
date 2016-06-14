require 'rfuse'
require 'pathname'
require_relative 'utils'

module StripeFS
	class FS
		attr_reader :chunksize, :stripes, :root, :rootstat
		
			
		def initialize(mountpath, stripes, chunksize)
			raise ArgumentError if Utils.is_blank?(stripes) || Utils.is_blank?(mountpath)
			
			@rootstat = ::Pathname.new(mountpath).lstat
			@root = ::Pathname.new(mountpath)
			raise ArgumentError unless @root.directory?
			
			@chunksize = chunksize
			@uid = ::Process.uid
			@gid = ::Process.gid

			@stripes = []
			Array(stripes).each_with_index do |stripe, index|
				@stripes[index] = ::Pathname.new(stripe)
				raise ArgumentError unless @stripes[index].directory?
			end
		end

		INIT_TIMES = Array.new(3,0)
		
		def lstat(pathname)
			return @rootstat if pathname.to_s == @root.to_s
			pathname.lstat
		end

		def method_missing(method, *args)
			puts method
			puts "method_missing"
		end

		def statfs(ctx,path)
			s = ::RFuse::StatVfs.new()
			s.f_bsize    = 1024
			s.f_frsize   = 1024
			s.f_blocks   = 1000000
			s.f_bfree    = 500000
			s.f_bavail   = 990000
			s.f_files    = 10000
			s.f_ffree    = 9900
			s.f_favail   = 9900
			s.f_fsid     = 23423
			s.f_flag     = 0
			s.f_namemax  = 10000
			return s
		end

		def typeofstat(type)
			case type
			when "directory"
			return ::RFuse::Stat::S_IFDIR
			when "file"
			return ::RFuse::Stat::S_IFREG
			when "link"
			return ::RFuse::Stat::S_IFLNK
			when "characterSpecial"
			return ::RFuse::Stat::IFCHR
			when "blockSpecial"
			return ::RFuse::Stat::IFBLK
			when "fifo"
			return ::RFuse::Stat::IFIFO
			when "socket"
			return ::RFuse::Stat::IFSOCK
			else
			return ::RFuse::Stat::S_IFMT
			end
		end

		def getstriped_paths(path)
			striped_paths = []
			pathname = ::Pathname.new(path)
			return striped_paths << @root.dup if pathname.root?
			
			basename = pathname.basename
			@stripes.each_with_index do |stripe, index|
				striped_paths[index] = stripe + "#{basename}.#{index+1}"
			end
			striped_paths
		end

		def getattr(ctx, path)
			striped_paths = getstriped_paths(path)
			stat = lstat(striped_paths[0])
			values = {:uid => @uid, :gid => @gid, :size => stat.size,
				:atime => stat.atime, :mtime => stat.mtime, :ctime => stat.ctime, :dev => stat.dev, :ino => stat.ino, 
				:nlink => stat.nlink, :rdev => stat.rdev, :blksize => stat.blksize, :blocks => stat.blocks}
			type = typeofstat(stat.ftype)
			return ::RFuse::Stat.new(type, stat.mode, values) if type == ::RFuse::Stat::S_IFDIR ||
																														type == ::RFuse::Stat::S_IFLNK

			striped_paths.each_with_index do |stripe, index|
				next if index == 0
				values[:size] += stripe.size
			end
			return ::RFuse::Stat.new(type, stat.mode, values)
		end
		
	end
end
