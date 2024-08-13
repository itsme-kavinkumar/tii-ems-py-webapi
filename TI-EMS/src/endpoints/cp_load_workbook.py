from openpyxl import load_workbook,Workbook
import re


def tokenize_formula(formula):
    # Define a regex pattern to match cell references, numbers, operators, parentheses, functions, and ranges
    pattern = r'([A-Z]+\d+|[A-Z]+:[A-Z]+|\d+\.\d+|\d+|[()+\-*\/,]|ROUND|AVERAGE|COUNT|MAX|MIN|IF|AND|OR|NOT|SUM)'
    # Find all matches
    tokens = re.findall(pattern, formula)
    # print(f"Tokenized formula: {tokens}")
    tokens = tokens.remove('SUM') if 'SUM' in tokens else tokens
    return tokens

def is_cell_reference(token):
    # Cell reference pattern (e.g., A1, B2, ...)
    cell_ref_pattern = r'[A-Z]+\d+'
    return re.match(cell_ref_pattern, token) is not None

def GetCellValue(formula, sheet):
    f_value = ''
    value = ''
    if f_value == '':
        split_formula = tokenize_formula(formula)
        cell_names = [data for data in split_formula if is_cell_reference(data)]
    
    while not isinstance(f_value, (int, float)):
        if f_value != '':
            split_formula = tokenize_formula(f_value)
            cell_names = [data for data in split_formula if is_cell_reference(data)]
        
        cl_value = ''
        value = ''
        for val in split_formula:
            if val in cell_names:
                # print(f"Evaluating cell reference: {val}")
                cl_value = sheet[f'{val}'].value
                # print(f"Cell value: {cl_value}")
                if isinstance(cl_value, str) and cl_value.startswith('=ROUND') and not cl_value.startswith('=ROUND(SUM'):
                    value += cl_value[6:-3]
                    # print(f"Processed ROUND: {cl_value[6:-3]}")
                elif isinstance(cl_value, str) and cl_value.startswith('=SUM'):
                    if ':' in formula:
                        formula = RangeCaculate(cl_value[1:])
                        value += formula[3:-3]
                    else:
                        value += cl_value[4:]
                    # print(f"Processed SUM: {value}")
                elif isinstance(cl_value, str) and cl_value.startswith('=ROUND(SUM'):
                    formula = RangeCaculate(cl_value[7:])
                    value += f'{formula[3:-3]})'
                    # print(f"Processed ROUND(SUM: {formula[3:-3]})")
                elif isinstance(cl_value, str) and cl_value.startswith('=') and not cl_value.startswith('=ROUND') and not cl_value.startswith('=SUM') and not cl_value.startswith('=ROUND(SUM'):
                    value += f'({cl_value[1:]})'
                    # print(f"Processed formula: {cl_value[1:]}")
                else:
                    if isinstance(cl_value, (int, float)):
                        # cl_value =  int(cl_value)
                        value += str(cl_value) 
                    else:
                         value += cl_value
                # print(f"Accumulated value: {value}")
            else:
                value += val
                # print(f"Accumulated value with non-cell reference: {value}")
        
        try:
            # index_val = split_formula.index(split_formula[-1])+1
            # if len(split_formula)== index_val:

            if not has_alphabets_in_math_expression(value):
                value  = value.replace('*/', '* 0 /')
                # print(f"Attempting to evaluate: {value}")
                eval_value = eval(value)
                f_value = eval_value if isinstance(eval_value, (int, float)) else f_value
                print(f"Evaluated value: {f_value}")
                f_value = round(f_value)
                # f_value = int(f_value)
                
            else:
                f_value = value
        except Exception as e:
            # print(f"Evaluation error: {e}")
            f_value = value
            # print(f"Non-evaluable value: {f_value}")
            

        if isinstance(f_value, (int, float)):
            break
        # print(f"Final value: {value}")
    
    return f_value

def RangeCaculate(formula):
    match = re.match(r'(\w+)\((.+)\)', formula)
    if match:
        function_name = match.group(1)
        arguments = match.group(2)

        # Split arguments by commas
        arg_list = arguments.split(',')

        arg_list_expanded = []
        for arg in arg_list:
            if ':' in arg:  # Check for range like G8:G13
                start, end = arg.split(':')
                start_col, start_row = re.match(r'([A-Z]+)(\d+)', start).groups()
                end_col, end_row = re.match(r'([A-Z]+)(\d+)', end).groups()

                if start_col == end_col:
                    col = start_col
                    for row in range(int(start_row), int(end_row) + 1):
                        arg_list_expanded.append(f"{col}{row}")
            else:
                arg_list_expanded.append(arg.strip())
                # return arg.strip()

        
        output_list = [function_name, '(']
        
        output_list.extend(['+', cell] if i > 0 else [cell] for i, cell in enumerate(arg_list_expanded))
        output_list.append(')')

        # print(f"Expanded range calculation: {''.join(''.join(x) for x in output_list)}")
        return ''.join(''.join(x) for x in output_list)

    return formula

def has_alphabets_in_math_expression(expression):
    # Remove all non-alphanumeric characters except spaces
    cleaned_expression = ''.join(c for c in expression if c.isalnum() or c == ' ')
    
    # Check for alphabetic characters
    return any(c.isalpha() for c in cleaned_expression)

# [ 'SUM', '(', 'G8','+','G9','+', 'G10','+','G11','+', 'G12','+','G13','+','G14','+','G15','+',':G16', ')']






