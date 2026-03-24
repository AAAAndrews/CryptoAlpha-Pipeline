for i in range(1,1000,1):
    exec("def get_{}(): return {}".format(i, i))


for i in range(1,1000,1):
    exec("def get_f{}(): return float({})".format(i, i))




def get_usual_constants():
    #Added floating point and integer versions of generating functions
    # for i in range(3,20,1):
    #     exec("def get_{}(): return {}".format(i, i))
    constant_function = {
        **{str(i): eval(f"[get_{i},[],int]") for i in range(3, 20)}
    }

    constant_function.update({
        **{"f"+str(i): eval(f"[get_f{i},[],float]") for i in range(1, 100)}
    })

    # for i in range(20,250,5):
    #     exec("def get_{}(): return {}".format(i, i))
    constant_function.update({
        **{str(i): eval(f"[get_{i},[],int]") for i in range(20, 250, 5)}
    })

    constant_function.update({
        **{"f"+str(i): eval(f"[get_f{i},[],float]") for i in range(20, 250, 5)}
    })





    return constant_function

def get_all_constants():

    constant_function = {
        **{str(i): eval(f"[get_{i},[],int]") for i in range(1,1000,1)}
    }
    constant_function.update({
        **{"f"+str(i): eval(f"[get_f{i},[],float]") for i in range(1,1000,1)}
    })
    return constant_function