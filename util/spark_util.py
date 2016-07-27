import os
def save_rdd(rdd, base_dir , hadoop_base_dir, filename):
    
    zip_dir = filename + '_dir'
    zip_file = filename + '.gz'
    local_zip_dir = os.path.join(base_dir, zip_dir)
    local_zip_file = os.path.join(base_dir, zip_file)
    hdfs_zip_dir = os.path.join(hadoop_base_dir, zip_dir)
    if not os.path.exists(base_dir):
        os.makedirs(base_dir)
    print (os.system( 'hadoop fs -mkdir ' + hadoop_base_dir))
    print (os.system('hadoop fs -rm -r ' + hdfs_zip_dir))
    print (os.system('rm -r ' + local_zip_dir))
    print (os.system('rm ' + os.path.join(base_dir, filename)))
    rdd.saveAsTextFile (hdfs_zip_dir , compressionCodecClass  =  "org.apache.hadoop.io.compress.GzipCodec")
    print (os.system('hadoop fs -copyToLocal ' + hdfs_zip_dir + ' ' + local_zip_dir))
    print (os.system( 'cat ' + local_zip_dir + '/*.gz > ' + local_zip_file))
    print (os.system( 'gunzip ' + local_zip_file))
    print (os.system('rm -r ' + local_zip_dir))

def get_parser(names):
    def loadRecord(line):
        cells = line.strip().split('\t')
        return dict(zip(names, cells))
    return loadRecord