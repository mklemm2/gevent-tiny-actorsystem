import function_pattern_matching as fpm
from function_pattern_matching import eq, ne, lt, le, gt, ge, Is, Isnot, isoftype, isiterable, eTrue, eFalse, In, notIn, _

def match(**kwargs):
	guard_kwargs = {k:v for k,v in kwargs.items() if isinstance(v, fpm.GuardFunc)}
	case_kwargs = {k:v for k,v in kwargs.items() if not isinstance(v, fpm.GuardFunc)}
	if case_kwargs and guard_kwargs:
		case_kwargs = {key:fpm.eq(val) for key, val in case_kwargs.items()}
		guard_kwargs.update(case_kwargs)
		case_kwargs = {}
	def decorator(decoratee):
		return (
			fpm.case(**case_kwargs)(fpm.guard(**guard_kwargs)(decoratee))
			if guard_kwargs
			else fpm.case(**case_kwargs)(decoratee)
		)
	return decorator

def default(decoratee):
	return fpm.case(decoratee)

def attr(attr, value=None, operation=None):
	def wrapper(arg):
		try:
			if operation and not value:
				return operation(getattr(arg, attr))
			elif value and not operation:
				return getattr(arg, attr) == value
			elif not operation and not value:
				return hasattr(arg, attr)
			else:
				return operation(getattr(arg, attr), value)
		except AttributeError:
			return False
	return fpm.GuardFunc(wrapper)

def item(item, value=None, operation=None):
	def wrapper(arg):
		try:
			if operation and not value:
				return operation(arg[item])
			elif value and not operation:
				return arg[item] == value
			elif not operation and not value:
				return item in arg
			else:
				return operation(arg[item], value)
		except KeyError:
			return False
	return fpm.GuardFunc(wrapper)
