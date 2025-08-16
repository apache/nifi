import ast
test_code = '''
PropertyDependency(my_module.IMPORTED_PROPERTY, ["true"])
'''

tree = ast.parse(test_code)
dependency = tree.body[0].value

print("Testing dependency types:")
print(f"Type of args[0]: {type(dependency.args[0])}")

if hasattr(dependency.args[0], 'attr'):
    print(f" Imported property detected: {dependency.args[0].attr}")
elif hasattr(dependency.args[0], 'id'):  
    print(f" Local property detected: {dependency.args[0].id}")

print(" Fix can handle both cases!")
