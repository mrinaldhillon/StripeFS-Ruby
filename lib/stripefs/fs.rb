require 'rfuse'
require 'pathname'
require "stringio"
require "fileutils"
require_relative "utils"

module StripeFS
	# Defines RFuse::Fuse file system apis to implement StripeFS
	#
	# @author Mrinal Dhillon
	# @todo link, symlink, readlink, mknod, access, getxattr, 
	#	setxattr, listxattr apis
	class FS
		#	Creates StripeFS::FS instance
		#
		#	@param mountpath [String] mountpoint for fuse filesystem
		#	@param stripes [Array<String>] stripe paths
		#	@param chunksize [Fixnum] chunksize for stripes
		# 
		#	@raise [ArgumentError]
		def initialize(mountpath, stripes, chunksize)
			fail ArgumentError, "Invalid Inputs" if Utils.is_blank?(stripes) || 
																					Utils.is_blank?(mountpath)
			
			@rootstat = Pathname.new(mountpath).lstat
			@root = Pathname.new(mountpath)
			fail ArgumentError, 
				"Mountpoint #{mountpath} is not a directory" unless @root.directory?
			
			@chunksize = chunksize
			@uid = Process.uid
			@gid = Process.gid

			@stripes = []
			Array(stripes).each_with_index do |stripe, index|
				@stripes[index] = Pathname.new(stripe)
				fail ArgumentError, 
				"Stripe target #{stripe} is not a directory" unless @stripes[index].directory?
			end
		end	#	initialize

		INIT_TIMES = Array.new(3,0)
		
		# Return stat info of object
		# @param path [::Pathname] path of object
		# 
		# @return [::Stat] stat of object
		def lstat(path)
			return @rootstat if path.to_s == @root.to_s
			path.lstat
		end	# lstat

		def method_missing(method, *args)
			$stderr << "#{method} is not implemented"
		end	# method_missing
		
		# Fuse statfs method. {RFuse::Fuse#statfs}
		# @param ctx [RFuse::Context]
		# @param path [String] object path
		# 
		#	@return [RFuse::StatVfs]
		#	@todo calculate based on stripe paths
		def statfs(ctx, path)
			s = RFuse::StatVfs.new()
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
		end	# statfs

		# Convert string to RFuse::Stat object type
		#	@param type [String]
		#
		#	@return [RFuse::Stat::Constant]
		def typeofstat(type)
			case type
			when "directory"
			return RFuse::Stat::S_IFDIR
			when "file"
			return RFuse::Stat::S_IFREG
			when "link"
			return RFuse::Stat::S_IFLNK
			when "characterSpecial"
			return RFuse::Stat::IFCHR
			when "blockSpecial"
			return RFuse::Stat::IFBLK
			when "fifo"
			return RFuse::Stat::IFIFO
			when "socket"
			return RFuse::Stat::IFSOCK
			else
			return RFuse::Stat::S_IFMT
			end
		end	# typeofstat
		
		# Utility function to convert StripeFS path to striped paths
		#	@param path [String] path to object
		#
		# @return [Array<String>] object path on stripes
		#	@todo write overload with stripe index
		#	@example
		#		StripeFS path /1/2/3 for n stripes corresponds to /1.1/2.1/3.1, 
		#		/1.2/2.2/2.3, ..., /1.n/2.n/3.n 
		def get_striped_paths(path)
			striped_paths = @stripes.dup
			return striped_paths if path == "/"
			
			pathname = Pathname.new(path)
			pathname.each_filename do |filename|
				striped_paths.each_with_index do |stripe, index| 
					striped_paths[index] = striped_paths[index] + "#{filename}.#{index+1}"
				end
			end
			
			striped_paths
		end	# get_striped_paths

		# Fuse getattr method. {RFuse::Fuse#getattr}
		# @param ctx [RFuse::Context]
		# @param path [String] object path
		#
		# @return [RFuse::Stat] stat info of object
		def getattr(ctx, path)
			if path == "/"
				stat = @rootstat.dup
			else
				striped_paths = get_striped_paths(path)
				stat = striped_paths[0].lstat
			end

			values = {:uid => @uid, :gid => @gid, :size => stat.size,
				:atime => stat.atime, :mtime => stat.mtime, :ctime => stat.ctime, 
				:dev => stat.dev, :ino => stat.ino, :nlink => stat.nlink, 
				:rdev => stat.rdev, :blksize => stat.blksize, :blocks => stat.blocks}
			type = typeofstat(stat.ftype)
			if type == RFuse::Stat::S_IFDIR || type == RFuse::Stat::S_IFLNK
				return RFuse::Stat.new(type, stat.mode, values) 
			end

			striped_paths.each_with_index do |stripe, index|
				next if index == 0
				values[:size] += stripe.size
			end
			return RFuse::Stat.new(type, stat.mode, values)
		end	# getattr

		# Fuse getattr method. {RFuse::Fuse#fgetattr}
		# @param ctx [RFuse::Context]
		# @param path [String] object path
		#	@param ffi [RFuse::FileInfo] object to store information of an 
		#																open file such file handle
		# 
		#	@return [RFuse::Stat] stat info of object
		def fgetattr(ctx, path, ffi)
			getattr(ctx, path)
		end	#	fgetattr

		# Fuse readdir method to list contents of directory. {RFuse::Fuse#readdir}
		# @param ctx [RFuse::Context]
		# @param path [String] object path
		#	@param filler [RFuse::Filler] structure to collect directory entries
		def readdir(ctx, path, filler, offset, ffi)
			striped_paths = get_striped_paths(path)
			
			 filler.push(".",nil,0)
			 filler.push("..",nil,0)

			 striped_paths[0].each_child(false) do |child|
			 	filler.push(child.to_s.chomp(".1"), nil, 0)
			end
		end	#	readdir

		# Fuse mkdir method to make directory. {RFuse::Fuse#mkdir}
		# @param ctx [RFuse::Context]
		# @param path [String] object path
		#	@mode mode [Fixnum] to obtain correct directory permissions
		def mkdir(ctx, path, mode)
			striped_paths = get_striped_paths(path)
			striped_paths.each { |stripe| stripe.mkdir(mode) }
		end	#	mkdir

		# Fuse rmdir method to remove empty directory. {RFuse::Fuse#rmdir}
		# @param ctx [RFuse::Context]
		# @param path [String] object path
		def rmdir(ctx, path)
			striped_paths = get_striped_paths(path)
			striped_paths.each { |stripe| stripe.rmdir }
		end
		
		# Fuse chmod method to change object permissions. {RFuse::Fuse#chmod}
		# @param ctx [RFuse::Context]
		# @param path [String] object path
		#	@mode mode [Fixnum] object permissions
		def chmod(ctx, path, mode)
			get_striped_paths(path).each { |stripe| stripe.chmod(mode) }
		end	#	chmod
		
		# Fuse chown method to change object ownenership. {RFuse::Fuse#chown}
		# @param ctx [RFuse::Context]
		# @param path [String] object path
		#	@mode uid [Fixnum] user id
		#	@mode gid [Fixnum] group id
		def chown(ctx, path, uid, gid)
			get_striped_paths(path).each { |stripe| stripe.chown(uid, gid) }
		end	#	chown
		
		# Fuse rename method to rename object. {RFuse::Fuse#rename}
		# @param ctx [RFuse::Context]
		# @param from [String] object path
		# @param to [String] object path
		def rename(ctx, from, to)
			striped_topaths = get_striped_paths(to)
			get_striped_paths(from).each_with_index do |stripe, index| 
				stripe.rename(striped_topaths[index].to_s)
			end
		end	#rename
		
		# Fuse open file method. {RFuse::Fuse#open}
		# @param ctx [RFuse::Context]
		# @param path [String] object path
		#	@param ffi [RFuse::FileInfo] stores file information such as file handle
		def open(ctx, path, ffi)
			get_striped_paths(path).each do |stripe| 
				stripe.open(ffi.flags) { |f| } # open and close file
			end
		end	#	open

		# Fuse creat file method. {RFuse::Fuse#creat}
		# @param ctx [RFuse::Context]
		# @param path [String] object path
		#	@mode mode [Fixnum] object permissions
		#	@param ffi [RFuse::FileInfo] stores file information such as file handle
		def create(ctx, path, mode, ffi)
			get_striped_paths(path).each do |stripe| 
				stripe.open("a+") do |f| 
					f.chmod(mode)
				end #open and close the file by calling open with block
			end
		end	#	creat
		
		#	Write at offset in stripe
		#	@param striped_paths [Array<String>]	array of striped paths
		#	@param io [StringIO] write buffer
		#	@param offset [Fixnum] offset in file
		#	@param chunksize [Fixnum] chunksize
		#	@param ffi [RFuse::FileInfo] stores file information such as file handle
		#
		#	@return [Fixnum] update offset
		def stripe_write(striped_paths, io, offset, chunksize, ffi)
			stripes_count = striped_paths.count
			
			# Find stripe number to begin writing
			chunks_till_offset_in_file = offset/chunksize
			stripe_at_offset = chunks_till_offset_in_file % stripes_count
			
			size = io.length - io.tell

			# Calculate Offset in Stripe to write at
			offset_in_chunk_in_stripe = offset % chunksize	#offset relative to a chunk 
			chunk_till_offset_in_stripe = chunks_till_offset_in_file / stripes_count
			offset_in_stripe = (chunk_till_offset_in_stripe * chunksize) + offset_in_chunk_in_stripe

			# Calculate Size of data to write at offset in stripe
			size_available_in_chunk = chunksize - offset_in_chunk_in_stripe
			size_to_write_in_stripe = size > size_available_in_chunk ? size_available_in_chunk : size
			
			# Write data of calcalated size at offset in stripe
			::File.open(striped_paths[stripe_at_offset], "r+") do |file|
				file.seek(offset_in_stripe, 0)
				offset += file.write(io.read(size_to_write_in_stripe))
			end
			# Return updated offset
			offset
		end	#	stripe_write

=begin

		Algorithm for #write
		
		1.Find which stripe to begin writing
		2. Find Offset in Split to write at.
		3. Calculate size to write based on chunk boundary
		4. If write size > chunk boundary offset than update write size and file offset repeat 1..4


		Steps
		1. Find out which stripe to begin writing
				ChunknumAtOffset = offset/chunksize #find number chunks till offset in the file
		2. SplitnumAtOffset= ChunknumAtOffset%SplitCount #find which stripe chunk and offset with lie
		2. Write can be across boundaries of stripes
		3. For each stripe with starting stripe
		1. Calculate offset in that stripe to start writing at
		1. OffsetInChunk (offset % chunk size) #find location of offset relative a single chunk’s boundary
		2. ChunknumInSplit = ChunknumAtOffset/SplitCount # gives no. chunks till offset in the stripe
		3. OffsetInSplit = (ChunknumInSplit*Chunksize) + OffsetInChunk
		2. Calculate Size to write at chunk in stripe
		1. SizeAvailableToWrite = Chunksize - OffsetInChunk
		2. if SizeToWrite > SizeAvailableToWrite
		1. then SizeInChunkAtSplit = SizeAvailableToWrite and SizeToWrite -= SizeInChunkAtSplit
		2. else SizeInChunkAtSplit = SizeToWrite
		4. Write at OffsetInSplit a buffer of SizeinChunkAtSplit

=end

		# Fuse write file method. {RFuse::Fuse#write}
		# @param ctx [RFuse::Context]
		# @param path [String] object path
		#	@param data [String] data buffer to write
		#	@param offset [Fixnum] offset in file to write at
		#	@param ffi [RFuse::FileInfo] stores file information such as file handle
		#
		# @return [Fixnum] number of bytes written
		def write(ctx, path, data, offset, ffi)
			chunksize = @chunksize
			striped_paths = get_striped_paths(path)
			offset_in_file = offset
			io = StringIO.new(data)
			size_of_buffer = io.length
			return 0 if 0 == io.length
			begin
				offset_in_file = stripe_write(striped_paths, io, offset_in_file, chunksize, ffi)
			end while io.tell != size_of_buffer
			
			length_written = offset_in_file - offset
			length_written
		end	#	write	
		
		#	Read at offset in stripe
		#	@param striped_paths [Array<String>] array of striped paths
		#	@param size [Fixnum] read size
		#	@param offset [Fixnum] offset in file
		#	@param chunksize [Fixnum] chunksize
		#	@param read_length [Fixnum] number of bytes read
		#	@param ffi [RFuse::FileInfo] stores file information such as file handle
		#
		# @return [String] read buffer
		def stripe_read(striped_paths, size, offset, chunksize, read_length, ffi)
			stripes_count = striped_paths.count
			# Calculate Stripe number corresponding to offset in file
			chunks_till_offset_in_file = offset/chunksize
			stripe_at_offset = chunks_till_offset_in_file % stripes_count

			size -= read_length
			read_buffer = ""
			# Calculate Offset in Stripe to read at
			offset_in_chunk_in_stripe = offset % chunksize	#offset relative to a chunk 
			chunk_till_offset_in_stripe = chunks_till_offset_in_file / stripes_count
			offset_in_stripe = (chunk_till_offset_in_stripe * chunksize) + offset_in_chunk_in_stripe

			# Calculate Size of data to read at offset in stripe
			size_to_read_in_chunk = chunksize - offset_in_chunk_in_stripe
			size_to_read_in_stripe = size > size_to_read_in_chunk ? size_to_read_in_chunk : size
			
			# Read data of calcalated size at offset in stripe
			::File.open(striped_paths[stripe_at_offset], "r+") do |file|
				file.seek(offset_in_stripe, 0)
				read_buffer = file.read(size_to_read_in_stripe)
			end
			read_buffer
		end	#	stripe_read

		# Fuse read file method. {RFuse::Fuse#read}
		# @param ctx [RFuse::Context]
		# @param path [String] object path
		#	@param offset [Fixnum] offset in file to read from
		#	@param ffi [RFuse::FileInfo] stores file information such as file handle
		#
		# @return [String] containing read bytes
		def read(ctx, path, size, offset, ffi)
			chunksize = @chunksize
			striped_paths = get_striped_paths(path)
			read_length = 0
			buffer = ""
			read_buffer = ""

			begin 
				read_buffer = stripe_read(striped_paths, size, offset, chunksize, read_length, ffi)
				if !read_buffer.nil?
					buffer << read_buffer 				
					len = read_buffer.length
					read_length += len
			 		offset += len
				end	
			end while (!read_buffer.nil? && read_length < size)
			buffer
		end	#	read
		
		# Fuse release method called once after #open. {RFuse::Fuse#release}
		# @param ctx [RFuse::Context]
		# @param path [String] object path
		#	@param ffi [RFuse::FileInfo] stores file information such as file handle
		def release(ctx, path, ffi) 
		end	#	release
		
		# Fuse unlink method to delete file. {RFuse::Fuse#release}
		# @param ctx [RFuse::Context]
		# @param path [String] object path
		def unlink(ctx, path)
			get_striped_paths(path).each { |path| path.unlink }
		end	#	unlink
		
		# Fuse utimens method to set file access and modification time. {RFuse::Fuse#utimens}
		# @param ctx [RFuse::Context]
		# @param path [String] object path
		# @param atime [Fixnum] access time
		# @param mtime [Fixnum] modified time
		def utimens(ctx, path, atime, mtime)
			get_striped_paths(path).each do |path| 
				path.utime(atime/1000000000, mtime/1000000000)
			end
		end	#	utimens
		
		# Fuse truncate method. {RFuse::Fuse#truncate}
		# @param ctx [RFuse::Context]
		# @param path [String] object path
		#	@param length [Fixnum] length to truncate 
		def truncate(ctx, path, length)
			striped_paths = get_striped_paths(path)
			stripes_count = striped_paths.count
			chunksize = @chunksize
			remaining_length = 0
			bytes_to_skip = 0
			stripe_final_size = []

			# Initialize truncate size for each stripe path to multiple 
			#												of chunksize for complete iteration
			(0..stripes_count-1).each_with_index do |index|
				break if length > (chunksize * stripes_count)
				stripe_final_size[index] = (length / (chunksize * stripes_count)) * chunksize
			end
		
			# Calculate final truncate size for last incomplete iteration through stripes
			remaining_length = length % (chunksize * stripes_count)

			(0..stripes_count-1).each_with_index do |index|
				break if remaining_length < 0
				bytes_to_skip = (remaining_length - chunksize) > 0 ? chunksize : remaining_length
				remaining_length -= bytes_to_skip;
				stripe_final_size[index] += bytes_to_skip;
			end

			striped_paths.each_with_index do |stripe, index|
				stripe.truncate(stripe_final_size[index])
			end
		end	#	truncate

		# Fuse ftruncate method. {RFuse::Fuse#ftruncate}
		# @param ctx [RFuse::Context]
		# @param path [String] object path
		#	@param length [Fixnum] length to truncate 
		#	@param ffi [RFuse::FileInfo] stores file information such as file handle
		def ftruncate(ctx, path, length, ffi)
			truncate(ctx, path, length)
		end	#	ftruncate

	end	#	FS
end	#	StripeFS
