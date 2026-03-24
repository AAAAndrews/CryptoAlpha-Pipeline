import openai
import json
import os
import pandas as pd
from typing import List, Dict, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
from copy import deepcopy
import threading

# Add thread lock to protect shared resources
# Configure OpenAI API[citation:1]
openai.api_key = os.getenv("OPENAI_API_KEY", "")  # Use environment variable instead of hardcoded secret
# Or use a newer version of the client [citation:10]
from openai import OpenAI
chatclient = OpenAI(api_key=os.getenv("OPENAI_API_KEY", ""),
                base_url="https://api.deepseek.com/v1")
MODEL = "deepseek-reasoner"
# NUM_FACTORS = 5 
from worldquant_ops import *

systen_prompt = """
You are a financial quantification researcher who is responsible for converting alpha factor expressions under different symbol systems into expressions under specific operator sets.

**Core mission: **
1. Analyze user-supplied alpha factor expressions for Alpha101 or other symbologies
2. Replace the operators in the expression with the target operator based on the user-provided operator set dictionary
3. Keep the mathematical logic and semantics of the expression unchanged
4. The output format must be strict JSON

**Conversion rules: **
1. Carefully match every operator, function, or operation in the source expression
2. If there is a corresponding mapping in the operator set, it is directly replaced by the target operator.
3. If there is no direct correspondence, try operator combination or approximate replacement
4. If it really cannot be replaced, explain it in the comments

**Output requirements: **
- Output only in JSON format without any extra text
- JSONThe following fields must be included:
  * "expression": Converted expression string
  * "mapping": Detailed description of each substitution
  * "unmapped": Parts that cannot be mapped directly
  * "validation": Expression validity check results

Output template:

[{{  "expression1": "mul(-1, ts_rank(ts_std(close, 20), 10))",
  "mapping": {{
    "ts_rank": "ts_rank",
    "ts_std": "ts_std",
    "*": "mul"
  }},
  "unmapped": [],
  "validation": {{
    "valid": true,
    "reason": "All operators have been successfully mapped",
    "syntax_check": "pass"
  }}
}},
{{  "expression2": "mul(-1, ts_rank(ts_std(close, 20), 10))",
  "mapping": {{
    "ts_rank": "ts_rank",
    "ts_std": "ts_std",
    "*": "mul"
  }},
  "unmapped": [],
  "validation": {{
    "valid": true,
    "reason": "All operators have been successfully mapped",
    "syntax_check": "pass"
  }}
}}
]


**My operator set situation**
{wq_legal_operator}

The factors that need to be converted may include the following operators:

abs(x),log(x),sign(x) = standard definitions
They are: take the absolute value, logarithmic value, sign (positive numbers return 1, negative numbers return -1)

rank(x) = cross-sectional rank
The ranking of the stock, the value ranges from 1 to the end. If the input value contains nan, then nan will not participate in the ranking, and the output is the boolean value of the corresponding ranking of the stock (the percentage of the total number of digits occupied by the ranking)

delay(x,d) = value of x d days ago
xThe value of the variable d days ago

corr/correlation(x,y,d) = time-serial correlation of x and y for the past d days
xand the correlation coefficient of the values ​​of the two variables y since d days

cov/covariance(x,y,d) = time-serial covariance of x and y for the past d days
xand the covariance of the values ​​of the two variables y since d days

scale(x,a) = rescaled x such that sum(abs(x))=a (the default is a=1)
Standardize the values ​​in x so that the sum of the absolute values ​​of x is a, default a=1

delta(x,d) = today’s value of x minus the value of x d days ago
The x value of the specified enddate minus the x value d days ago

signedpower(x,a) = x^a
xThe value raised to the power a. If x is a list or series, it is the power a of each value in x.

decay_linear(x,d) = weighted moving average over the past d days with linearly decaying weights d,d-1,…,1 (rescaled up to 1)
xThe values ​​from the farthest to the nearest time in the middle are multiplied by the weights d and d-1 respectively.…，1（The weights need to be standardized so that the sum is 1) and then summed

ts_min(x,d) = time-series min over the past d days
xThe smallest value within d days

ts_max(x,d) = time-series max over the past d days
xThe maximum value within d days

ts_argmin(x,d) = which day ts_min(x,d) occurred on
ts_min(x,d)The number of days it occurs in d days, the farthest day is the first day

ts_argmax(x,d) = which day ts_max(x,d) occurred on
ts_max(x,d)The number of days it occurs in d days, the farthest day is the first day

ts_rank(x,d) = time-series rank in the past d days
x, the value of the last day, how many rankings were ranked in these d days, and the final output ranking is a boolean value (that is, the ranking accounts for the percentage of the total rankings)

min(x,d) = ts_min(x,d)
When encountering the min function, when the ts_min function processes——-Notice! ! In fact, when min is encountered, when the input in min is not (x, d) but (x, y), the minimum value of the two values ​​​​of x and y is taken.

max(x,d) = ts_max(x,d)
When encountering the max function, when the ts_max function processes——Notice! ! In fact, when max is encountered, when the input in max is not (x, d) but (x, y), the maximum value of the two values ​​​​of x and y is taken.

sum(x,d) = time-series sum over the past d days
dThe sum of x values ​​since days

product(x,d) = time-series product over the past d days
dproduct of x values ​​since days

stddev(x,d) = moving time series standard deviation over the past d days
dThe standard deviation of x values ​​over days

Notes on conversion are as follows:
For operators with similar names and similar input data types, observe the small differences between them, such as maybe my operator is concentrated
The definition of standard deviation (timing) is: ts_std_dev, and what is to be converted is stddev
The definition of correlation (timing) is: ts_corr, and what is to be converted is correlation/CORR
The definition of covariance (time series) is: ts_cov, and what is to be converted is covariance/COV
The definition of the minimum value index of the time series is: ts_arg_min, and what is to be converted is ts_argmin
There are also some such as SMA that can be replaced by ts_mean.
For some operators that cannot be mapped directly, you can try to use a combination of multiple operators to achieve similar functions, such as
multiply(a,b,c,...) This can be achieved using multiply(a, mul(b, multiply(c,...)))

In addition, pay full attention to the data types that can be passed in. If it is a field name (such as an English word), it is regarded as ndarray,
If it is a number, it is treated as float or int.


Use appropriate operators to perform operations, and pay attention to the type order of the parameters passed in. Do not reverse the order, otherwise the function cannot be calculated.
For example -2*x, the incoming -2 should be regarded as float-2.0, and x should be regarded as ndarray, and the corresponding operator is multiply_fn
x*-2,The incoming -2 should be regarded as float-2.0, and x should be regarded as ndarray, and the corresponding operator is multiply_nf




In addition, -1*x can be implemented using reverse(x)
1/xThis can be achieved using inverse(x)
This simplifies calculations


If my operator only allows floating point numbers to be passed in, and the expression to be converted has integers passed in, I can convert the integer to a floating point number.
If the operator only allows integers to be passed in, it should not be converted.
"""
systen_prompt = systen_prompt.format(wq_legal_operator=wq_legal_operator)
import joblib
from tqdm import tqdm as tqdm
import os
if __name__ == "__main__":
    alpha = joblib.load("Cross_Section_Factor/factorset/numericalpha_str_alpha191.pkl")
    for a in tqdm(alpha,desc="The alpha factor to be converted has been read and is in AI conversion format."):

      user_prompt = f"Multiple alphas have been separated by semicolons, please convert:{a}"
      response = chatclient.chat.completions.create(
          model=MODEL,
          messages=[
              {"role": "system", "content": systen_prompt},
              {"role": "user", "content": user_prompt}
          ],
          temperature=0.7,
          response_format={"type": "json_object"}  # Force JSON output
      )
      result = json.loads(response.choices[0].message.content)
      if "wq_formatted_alpha191.pkl" not in os.listdir("Cross_Section_Factor/factorset/"):
          result = [*result]
      else:
          old_result = joblib.load("Cross_Section_Factor/factorset/wq_formatted_alpha191.pkl")
          result = [*old_result, *result]
      joblib.dump(result, "Cross_Section_Factor/factorset/wq_formatted_alpha191.pkl")
      # print(f"Results of this round of conversion:{result}")