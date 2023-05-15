//  Created by Lin Teng on 19/01/2022.

import os
import sys
import time
import re
import math
import pandas as pd
from natsort import ns, natsorted

def my_log(info, over_write=False):
    log_string = time.asctime() + " " +info+"\n"
    print(log_string)

    try:
        if over_write:
            with open('compare_log.txt', 'w') as f:
                f.write(log_string)
                f.close()
        else:
            with open('compare_log.txt', 'a+') as f:
                f.write(log_string)
                f.close()
    except Exception as e:
        print('写入错误日志时发生以下错误：\n%s'%e)

def is_number(s):
    try:
        float(s)
        if(float(s)!=float(s)):
            return False
        else :
            return True
    except ValueError:
        pass
    #
    # try:
    #     import unicodedata
    #     unicodedata.numeric(s)
    #     return True
    # except (TypeError, ValueError):
    #     pass
    return False


def to_float(s):
    if is_number(s):
        return float(s)
    else:
        return -100000.0


def delete_first_two_lines_of_file(charge_raw_file):
    dir_path = os.getcwd()

    charge_raw_path, charge_short_file_name = os.path.split(charge_raw_file)
    comparing_path = os.path.join(dir_path, "comparing_files")

    charge_full_file_name = charge_raw_file
    charge_comparing_full_file_name = os.path.join(comparing_path, charge_short_file_name)
    ###print(charge_full_file_name)
    ###print(charge_comparing_full_file_name)

    try:
        with open(charge_full_file_name, 'r') as read_file:
            contents = read_file.readlines()
            contents_main = contents[2:]
            read_file.close()
            with open(charge_comparing_full_file_name, 'w') as write_file:
                for content_line in contents_main:
                    write_file.write(content_line)
                write_file.close()
    except Exception as e:
        my_log("error:  delete_first_two_lines_of_file: " + charge_raw_file)

def calc_dataframes(description_list, df_pure_raw_data, df_charge_raw_data):
    # 按水平方向合并两个dataframe
    #df_ch = pd.concat([df_pure_raw_data, df_charge_raw_data], axis=1, ignore_index=True)
    # df_ch = pd.concat([df1, df2])
    #print(df_ch)

    result_list_of_list = []
    for x in range(len(df_pure_raw_data)):
        ###print(str(x) + "##:")
        #print(df_pure_raw_data.iloc[x].tolist())
        list_pure_raw = df_pure_raw_data.iloc[x].tolist()
        list_charge_raw = df_charge_raw_data.iloc[x].tolist();
        list_pure_later_part = list_pure_raw[1:]
        list_charge_pre_part = list_charge_raw[:len(list_charge_raw)-1]
        list_charge_later_part = list_charge_raw[1:]
        ###print(list_pure_later_part)
        ###print(list_charge_pre_part)
        ###print(list_charge_later_part)

        result_list = []
        for (pure_later_item, charge_pre_item, charge_later_item) in zip(list_pure_later_part, list_charge_pre_part, list_charge_later_part):
            result_item = to_float(pure_later_item) - (to_float(charge_later_item) - to_float(charge_pre_item))
            if(math.fabs(result_item) > 10000):
                # something wrong, please check the file format!!!
                my_log("something wrong, please check the file format!!!")
                time.sleep(1)
                sys.exit(1)
            result_list.append(result_item)
        #print(result_list)
        result_list_of_list.append(result_list)
    #print(result_list_of_list)

    result_list_of_list.insert(0,description_list)
    result_list_of_list.append(["end ", "---"])
    df_target = pd.DataFrame(result_list_of_list)
    #print(df_target)

    return df_target

def filename_number_is_equal(pure_raw_file, charge_raw_file):
    number1 = re.findall("\d+",pure_raw_file)[0]
    number2 = re.findall("\d+", charge_raw_file)[0]
    #print("filename_number_is_equal" + str(number1) + "  " + str(number2))
    return number1 == number2

def compare_pure_and_other_file(pure_raw_file, charge_raw_file, pure_section):


    my_log(" Info : start to compare files: " + pure_raw_file + " and " + charge_raw_file)


    dir_path = os.getcwd()
    pure_raw_path = os.path.join(dir_path, "pureraw")
    charge_raw_path = os.path.join(dir_path, "rawdata")
    #charge_raw_path = os.path.join(dir_path, "comparing_files")
    comparing_path = os.path.join(dir_path, "comparing_files")


    pure_full_file_name = os.path.join(pure_raw_path, pure_raw_file)
    pure_raw_file_basename, ext = os.path.splitext(os.path.basename(pure_raw_file))
    pure_xlsx_short_file_name = pure_raw_file_basename + ".xlsx"
    pure_xlsx_full_file_name = os.path.join(comparing_path, pure_xlsx_short_file_name)
    ###print(pure_full_file_name)
    ###print(pure_xlsx_full_file_name)

    charge_full_file_name = os.path.join(charge_raw_path, charge_raw_file)
    charge_raw_file_basename, ext = os.path.splitext(os.path.basename(charge_raw_file))
    charge_xlsx_short_file_name = charge_raw_file_basename + ".xlsx"
    charge_xlsx_full_file_name = os.path.join(comparing_path, charge_xlsx_short_file_name)
    ###print(charge_full_file_name)
    ###print(charge_xlsx_full_file_name)

    # read the csv files
    df1 = pd.read_csv(pure_full_file_name, sep=",")
    df2 = pd.read_csv(charge_full_file_name, sep=",")

    # convert to xlsx files, and write to comparing_files dir
    df1.to_excel(pure_xlsx_full_file_name)
    df2.to_excel(charge_xlsx_full_file_name)

    # read the two xlsx files from the comparing_files dir
    df1 = pd.read_excel(pure_xlsx_full_file_name)
    df2 = pd.read_excel(charge_xlsx_full_file_name)

    #print(df1)
    #print(df2)

    # No1
    description_list = [pure_raw_file, "MRN[0]", "MRN0[0]", charge_raw_file ]
    df_pure_setction1 = df1.iloc[1:17,1:37]
    df_charge_section1 = df2.iloc[1:17, 1:37]
    df_result_section1 = calc_dataframes(description_list, df_pure_setction1.reset_index(drop=True), df_charge_section1.reset_index(drop=True))
    ### print(df_result_section1)

    # No2
    description_list = [pure_raw_file, "MRN[1]", "MRN1[0]", charge_raw_file]
    df_pure_setction2 = df1.iloc[19:35, 1:37]
    df_charge_section2 = df2.iloc[91:107, 1:37]
    df_result_section2 = calc_dataframes(description_list, df_pure_setction2.reset_index(drop=True),
                                         df_charge_section2.reset_index(drop=True))
    ### print(df_result_section2)

    # No3
    description_list = [pure_raw_file, "MRN[2]", "MRN2[0]", charge_raw_file]
    df_pure_setction3 = df1.iloc[37:53, 1:37]
    df_charge_section3 = df2.iloc[181:197, 1:37]
    df_result_section3 = calc_dataframes(description_list, df_pure_setction3.reset_index(drop=True),
                                         df_charge_section3.reset_index(drop=True))
    ### print(df_result_section3)

    # 合并 df_result_section1, df_result_section2, df_result_section3
    df_result_one_file_all = df_result_section1.append(df_result_section2)
    df_result_one_file_all = df_result_one_file_all.append(df_result_section3)
    #
    return  df_result_one_file_all

def compare_func():
    # get current work path
    dir_path = os.getcwd()

    # get the file name list of pure_raw
    pure_raw_path = os.path.join(dir_path, "pureraw")
    pure_raw_files_unsorted = []
    for item in os.listdir(pure_raw_path):
        if os.path.isfile(os.path.join(pure_raw_path, item)):
            pure_raw_files_unsorted.append(item)
    # pure_raw_files.sort(key=lambda x: int(x[8:-4]))
    pure_raw_files = natsorted(pure_raw_files_unsorted, alg=ns.PATH)
    print(pure_raw_files)

    # get the file name list of rawdata
    raw_data_path = os.path.join(dir_path, "rawdata")
    raw_data_files_unsorted = []
    for item in os.listdir(raw_data_path):
        if os.path.isfile(os.path.join(raw_data_path, item)):
            raw_data_files_unsorted.append(item)
    raw_data_files = natsorted(raw_data_files_unsorted, alg=ns.PATH)
    print(raw_data_files)

    # check if the length of file name lists are same
    if len(pure_raw_files) != len(raw_data_files):
        my_log("Error: the count of pureraw files is not same with the count of rawdata files!")
        sys.exit(1)

    output_path = os.path.join(dir_path, "output_files")

    ##### now compare pure_raw with rawdata
    df_result_raw_data = compare_pure_files_with_other_files(pure_raw_files, raw_data_files, -1)
    # #write_df_to_output
    target_raw_data_file_name = os.path.join(output_path, "raw_data_resluts.xlsx")
    df_result_raw_data.to_excel(target_raw_data_file_name)

def compare_pure_files_with_other_files(pure_raw_files, other_raw_files, pure_section):
    df_result_compare_other_all = pd.DataFrame()
    for (pure_raw_file1, other_raw_file1) in zip(pure_raw_files, other_raw_files):
        df_result_compare_other_all = df_result_compare_other_all.append(
            compare_pure_and_other_file(pure_raw_file1, other_raw_file1, pure_section))
    return df_result_compare_other_all

if __name__ == '__main__':
    my_log("Start to compare the data in files ...", True)

    # check if the dir pureraw is ok
    if not os.path.exists("pureraw"):
        error_msg = "error: pureraw dir is not found, please check!"
        my_log(error_msg)
        sys.exit(1)
    # check if the dir rawdata is ok
    if not os.path.exists("rawdata"):
        error_msg = "error: rawdata dir is not found, please check!"
        my_log(error_msg)
        sys.exit(1)

    if not os.path.exists("comparing_files"):
        try:
            os.mkdir("comparing_files")
        except Exception as e:
            my_log(e)
            sys.exit(1)

    if not os.path.exists("output_files"):
        try:
            os.mkdir("output_files")
        except Exception as e:
            my_log(e)
            sys.exit(1)
    try:
        compare_func()
    except Exception as e:
        my_log('Exception ：\n%s' % e)
        sys.exit(1)
    my_log("Finished! Everthing is ok.")
    sys.exit(0)


