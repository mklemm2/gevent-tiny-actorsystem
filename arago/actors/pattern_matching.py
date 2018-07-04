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

