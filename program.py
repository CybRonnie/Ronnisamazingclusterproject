from pyspark.sql import SparkSession
import random


def map_paragraphs(item):
    # get paragraphs
    dlst = item['paragraphs']
    output = []
    for p in dlst:
        # build paragraph item object, add contract id field
        output.append({
            'contract_id': item['title'],
            'context': p['context'], # add contract text
            'qas': p['qas'], # add question and answers
        })
    return output


def map_to_quetion_answers(x):
    # get contract id
    contract_id = x['contract_id']
    # get contract text
    contract = x['context']
    # get question and answers
    questions = x['qas']
    # the ouptut list
    output = []
    for question in questions:
        # get answers
        answers = question['answers']
        # get question text
        question_text = question['question']
        # is impossible flag
        is_impossible = question['is_impossible']
        for answer in answers:
            # get answer start position
            answer_start = answer['answer_start']
            # get answer text
            answer_text = answer['text']
            # build a object contains contract info and question answer info
            item = {
                'contract_id': contract_id,
                'contract': contract,
                'question': question_text,
                'answer_start': answer_start,
                'text': answer_text,
                'is_impossible': is_impossible
            }
            output.append(item)
    return output


def create_ranges(text_length, window, stride):
    '''split a length to ranges'''
    result = []
    start = 0
    while start < text_length:
        # calculate end position
        end = start + window
        # check if out of range
        if end > text_length:
            # set to end, if out of range
            end = text_length
        # add to result
        result.append((start, end))
        # slide to next position
        start += stride
    return result


def overlap(range_start, range_end, answer_start, answer_end):
    '''check if the answer range overlap with a window'''
    if answer_start >= range_end:
        return False
    if answer_end < range_start:
        return False
    return True


def map_to_window_question(item):
    '''use slide window to split contract into same length'''
    output = []
    # retrieve question info
    is_impossible = item['is_impossible']
    contract_id = item['contract_id']
    contract = item['contract']
    question = item['question']
    answer_start = item['answer_start']
    answer_end = answer_start + len(item['text'])
    # split contract text
    for start, end in create_ranges(len(contract), 512, 256):
        source = contract[start: end]
        if not is_impossible:
            if overlap(start, end, answer_start, answer_end): # positive
                output.append((contract_id, {
                    'source': source,
                    'question': question,
                    'answer_start': max(answer_start, start) - start,
                    'answer_end': min(answer_end, end) - start,
                    'range_start': start,
                    'range_end': end,
                    'type': 'positive'
                }))
            else:  # possible negative
                output.append((contract_id, {
                    'source': source,
                    'question': question,
                    'answer_start': 0,
                    'answer_end': 0,
                    'range_start': start,
                    'range_end': end,
                    'type': 'possible'
                }))
        else:
            # impossible negative
            output.append((contract_id, {
                'source': source,
                'question': question,
                'answer_start': 0,
                'answer_end': 0,
                'range_start': start,
                'range_end': end,
                'type': 'impossible'
            }))
    return output


def map_dataset(questions, positive_question_count_dict, contract_count):
    '''generate dataset for each contract'''
    import random
    impossible_list = []
    possible_list = []
    positive_list = []
    positive_range_set = set()
    impossible_map = {}
    # calculate total positive answer count
    total_positive_count = 0
    for question_text, count in positive_question_count_dict.items():
        total_positive_count += count

    for question in questions:
        question_type = question['type']
        if question_type == 'positive':
            # build positive question answer list
            positive_list.append(question)
            positive_range_set.add((question['range_start'], question['range_end']))
        elif question_type == 'possible':
            # build possible negative question answer list
            possible_list.append(question)
        elif question_type == 'impossible':
            # build impossible negative question answer dict
            if question['question'] not in impossible_map:
                impossible_map[question['question']] = []
            impossible_map[question['question']].append(question)

    # covered ranges for possible negative and impossible negative
    possible_range_set = set()
    impossible_range_set = set()

    output_possible_list = []
    output_impossible_list = []

    # randomize
    random.shuffle(impossible_list)
    random.shuffle(possible_list)

    # build impossible negative list
    impossible_list = []
    for question_text in impossible_map:
        # get current question count of this contract
        questions = impossible_map[question_text]
        # get all count of this question
        cnt = positive_question_count_dict[question_text]
        # other contract question answer count
        average = total_positive_count - cnt
        # average of other
        average = int(average / (contract_count - 1))
        # add average count of impossible negative
        impossible_list += questions[:average]

    for question in impossible_list:
        # if not in positve range
        if (question['range_start'], question['range_end']) not in positive_range_set:
            # if not in current impossible range
            if (question['range_start'], question['range_end']) not in impossible_range_set:
                impossible_range_set.add((question['range_start'], question['range_end']))
                output_impossible_list.append(question)
        if len(output_impossible_list) >= len(positive_list):
            break


    for question in possible_list:
        # if not in positve range
        if (question['range_start'], question['range_end']) not in positive_range_set:
            # if not in current possible range
            if (question['range_start'], question['range_end']) not in possible_range_set:
                # add to output
                possible_range_set.add((question['range_start'], question['range_end']))
                output_possible_list.append(question)
        if len(output_possible_list) >= len(positive_list):
            break

    # build output
    output = []
    for question in output_possible_list + output_impossible_list + positive_list:
        output.append({
            'source': question['source'],
            'question': question['question'],
            'answer_start': question['answer_start'],
            'answer_end': question['answer_end'],
        })
    return output

# create session
spark = SparkSession \
    .builder \
    .appName("Assignment2") \
    .getOrCreate()


def process(input_file, output_file):
    # read json file
    dataframe = spark.read.json(input_file)
    rdd = dataframe.rdd
    # flatmap paragraphs
    rdd1 = rdd.flatMap(lambda x: x['data']) \
        .flatMap(map_paragraphs)

    # flatmap questions
    rdd2 = rdd1.flatMap(map_to_quetion_answers)
    # flatmap slide window questions
    rdd3 = rdd2.flatMap(map_to_window_question)
    # group by contract id
    rdd4 = rdd3.groupByKey()
    # calculate positive answer count list foreach question
    positive_question_count_list = rdd4.flatMap(lambda item: item[1]) \
        .filter(lambda x: x['answer_start'] != 0 and x['answer_end'] != 0) \
        .map(lambda x: (x['question'], x)) \
        .groupByKey() \
        .mapValues(lambda x: len(x)) \
        .collect()
    # convert list to dictionary
    positive_question_count_dict = {}
    for question, count in positive_question_count_list:
        positive_question_count_dict[question] = count
    # get contract count
    contract_count = rdd1.count()
    # generate dataset
    rdd5 = rdd4.mapValues(lambda x: map_dataset(x, positive_question_count_dict, contract_count))
    rdd5.toDF().write.mode('overwrite').json(output_file)

# process CUADv1.json
process("CUADv1.json", 'CUADv1_output.json')

# process test.json
# process("test.json", 'test_output.json')
