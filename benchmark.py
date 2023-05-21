import os
import sys

import shutil
sys.path.append("../")
from logzip.logzipper import Ziplog
from subprocess import check_output
import csv

datasets = {
    "android": "<Date> <Time> <Pid> <Tid> <Level> <Component>: <Content>",
    "apache": "\\[<Time>\\] \\[<Level>\\] <Content>",
    "BGL": "<Label> <Timestamp> <Date> <Node> <Time> <NodeRepeat> <Type> <Component> <Level> <Content>",
    "Hadoop": "<Date> <Time> <Level> \\[<Process>\\] <Component>: <Content>",
    "HDFS": "<Date> <Time> <Pid> <Level> <Component>: <Content>",
    "HealthApp": "<Time>\\|<Component>\\|<Pid>\\|<Content>",
    "HPC": "<LogId> <Node> <Component> <State> <Time> <Flag> <Content>",
    "Linux": "<Month> <Date> <Time> <Level> <Component>: <Content>",
    "Mac": "<Month>  <Date> <Time> <User> <Component>: <Content>",
    "OpenSSH": "<Date> <Day> <Time> <Component> <SSHD>: <Content>",
    "OpenStack": "<Logrecord> <Date> <Time> <Pid> <Level> <Component> \\[<ADDR>\\] <Content>",
    "Proxifier": "\\[<Time>\\] <Program> - <Content>",
    "Spark": "<Date> <Time> <Level> <Component>: <Content>",
    "Windows": "<Date> <Time>, <Level> <Component> <Content>",
    "Zookeeper": "<Date> <Time> - <Level>  \\[<Node>:<Component>@<Id>\\] - <Content>",
}
# datasets = {
#     "HealthApp": "<Time>\\|<Component>\\|<Pid>\\|<Content>",
# } 

# Uncomment this line to process only the "Thunderbird" dataset
parsers = ["Revision", "VTE", "Drain"]
# parsers = ["Revision"]


level = 3
kernel = "bz2"   # options: (1) gz  (2) bz2
n_workers = 12

os.makedirs("archives", exist_ok=True)


results = []
for parser in parsers:
    for dataset, logformat in datasets.items():
        archive_path = os.path.join(os.getcwd(), "archives", parser ,dataset)
        tmp_dir = os.path.join(os.getcwd(), "archives", parser ,dataset, "tmp")
        outname = dataset + ".logzip"
        templates_filepath = os.path.join(os.getcwd(), "templates", parser, dataset+ "_2k.log_templates.csv")
        
        shutil.rmtree(archive_path, ignore_errors=True)
        os.makedirs(archive_path)

        filepath = f"logs/{dataset}/{dataset}.log"
        
        zipper = Ziplog(logformat=logformat,
                        outdir=archive_path,
                        outname=outname,
                        kernel=kernel,
                        tmp_dir=tmp_dir,
                        level=level)
        zipper.zip_file(filepath, templates_filepath)
        # Get the size of the file in bytes
        file_size = os.path.getsize(os.path.join(archive_path, outname + ".tar." + kernel))

        # Print the file size to stdout
        result = {
            "parser": parser,
            "dataset": dataset,
            "file_size": file_size
        }
        # Append the result dictionary to the results list
        results.append(result)
        

for result in results:
    print(result)

# write to csv

with open("results.csv", "w", newline="") as csvfile:
    fieldnames = results[0].keys()
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    writer.writeheader()  # Write the header row with field names
    for data in results:
        writer.writerow(data)  # Write each data object as a row
   