for obj in my_bucket.objects.filter(Prefix=s3filePath):
	s3FileToProcess = "s3://" + s3BucketName + s3FilePathDelimiter + obj.key
	head, tail = os.path.split(obj.key)
	File = tail	
	if (File != 'Success.dsv'):		
		Filesplit = File.split('.')
		filename = Filesplit[0] + '.' + Filesplit[1]	
		df_profile = pd.read_csv(s3FileToProcess,sep = '|',low_memory=False)
		out = Data_Profiling(df_profile,filename,s3DateFolder)
		out['PROCESSED_DATE'] = s3DateFolder
		out['PROCESSED_DATE_CONVERTED'] = pd.to_datetime(out['PROCESSED_DATE'], format='%Y%m%d')
		out['year'] = out['PROCESSED_DATE_CONVERTED'].apply(lambda x: x.year)
		s3 = S3FileSystem()
		pq.write_to_dataset(table=table,root_path=s3ParquetFilePath,partition_cols=['year','month','day'],filesystem=s3,use_dictionary=True)		
	print("Sucessfully File loaded to s3 Bucket:" + s3ParquetFilePath + filename)
