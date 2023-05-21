import sys
sys.path.append("../")
from logzip.logzipper import Ziplog

if __name__ == "__main__":
    filepath = "../logs/HDFS/HDFS.log"
    templates_filepath = "../logs/Drain_result/HDFS_2k.log_templates.csv"  # templates file, one template per line

    out_dir = "../zip_out/"
    outname = "HDFS" + ".logzip"
    tmp_dir = "../zip_out/tmp_dir"

    level = 2
    kernel = "bz2"   # options: (1) gz  (2) bz2
    n_workers = 1
    
    zipper = Ziplog(logformat="<Date> <Time> <Pid> <Level> <Component>: <Content>",
                    outdir=out_dir,
                    outname=outname,
                    kernel=kernel,
                    tmp_dir=tmp_dir,
                    level=level)
    zipper.zip_file(filepath, templates_filepath)
    