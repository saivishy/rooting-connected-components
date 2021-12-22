# !/usr/bin/python

from pyspark import SparkConf, SparkContext
import time
import sys

def vertex_collective(a,b):
    a.append(b[0])
    return a

def in_out_intial(line):
    l = line.split(" ")
    # True -  out edge
    # False - in edge
    return [
        (
            (int(l[0])),
            (True, (int(l[1])))
        ),
        (
            (int(l[1])),
            (False, (int(l[0])))
        )
    ]

def in_out_groupby(key_val):

    # True -  out going edge
    # False - in coming edgesr
    v = key_val[1]
    return [
        (
            int(key_val[0]),
            (True, v[1])
        ),
        (
            int(v[1]),
            (False, (key_val[0])) 
        )
    ]


def pointer_jump_groupby(key_val):
    gamma_u = key_val[1]
    emmit_list = []
    if len(gamma_u)>1:
        for vertex in gamma_u:
            if vertex[0]:
                for other_vertex in gamma_u:
                    if other_vertex[0] == False:
                        emmit_list.append(
                            (other_vertex[1],(True, vertex[1]))
                            )

    return emmit_list

def final_form(key_val):
    return (key_val[0], key_val[1][1])

if __name__ == "__main__":
    conf = SparkConf().setAppName("RDDcreate")
    start_time = time.time()

    sc = SparkContext(conf=conf)
    lines = sc.textFile(sys.argv[1])

    pointer_jump = lines.flatMap(in_out_intial).groupByKey().mapValues(list).flatMap(pointer_jump_groupby)

    in_out_rep = pointer_jump.flatMap(in_out_groupby)
    in_out_rep.persist()
 
    OK = in_out_rep.map(final_form)
    OK.persist()

    checksum = OK.values().sum()

    i=1
    while True:

        """

            Algorithm Overview:

            groupbykey() - Collect All neighbours
            map() - Point all neighbours to Parent , return pointed pairs
            flatmap(pointer_jump)
            map().values().sum() - for sum
            flatMap(vertex neighbourhood)

            Repeat until (previous)checksum==sum(new)

            Return Final Map

        """

        initial = in_out_rep.groupByKey().mapValues(list)

        # if (i==19):  # Repartition if needed
        #     initial.partitionBy(40)
        
        pointer_jump = initial.flatMap(pointer_jump_groupby)
        pointer_jump.persist()

        KO = pointer_jump.map(final_form)
        KO.persist()

        new_sum = KO.values().sum()
        if checksum==new_sum:
            KO.saveAsTextFile(sys.argv[2] + f"_endIter{i}")       
            break
        in_out_rep = pointer_jump.flatMap(in_out_groupby)
        in_out_rep.persist()
        
        # get checksum
        checksum = new_sum
        i+=1
    # wait = input()
    sc.stop()
    end_time = time.time()
    runtime = end_time - start_time
    print("File Saved at : "+sys.argv[2])
    print("Runtime:",runtime)
    file1 = open("time.txt","a")
    file1.write("\n"+sys.argv[2]+":"+str(runtime)+"\n")
    file1.close()
