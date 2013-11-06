"""
    Let us use newer collections classes in other version of python.  Tries to import from the collections
    module.  If it fails it uses copied versions or a pure python version.
"""

import logging 


log = logging.getLogger(__name__)

try:
    all()
except:
    def all(iterable):
        """ Need to define all as it doesn't exist in python 2.4, what does exist in 2.4? """
        for x in iterable:
            if not x: return False
        return True

try:
    from collections import namedtuple
except:
    from operator import itemgetter as _itemgetter
    from keyword import iskeyword as _iskeyword
    import sys as _sys
    
    # Copied directly from python 2.6 collections.py to provide named tuple when running under older
    #  python instances such as 2.5, commented out other missing imports
    def namedtuple(typename, field_names, verbose=False):
        """Returns a new subclass of tuple with named fields.
    
        >>> Point = namedtuple('Point', 'x y')
        >>> Point.__doc__                   # docstring for the new class
        'Point(x, y)'
        >>> p = Point(11, y=22)             # instantiate with positional args or keywords
        >>> p[0] + p[1]                     # indexable like a plain tuple
        33
        >>> x, y = p                        # unpack like a regular tuple
        >>> x, y
        (11, 22)
        >>> p.x + p.y                       # fields also accessable by name
        33
        >>> d = p._asdict()                 # convert to a dictionary
        >>> d['x']
        11
        >>> Point(**d)                      # convert from a dictionary
        Point(x=11, y=22)
        >>> p._replace(x=100)               # _replace() is like str.replace() but targets named fields
        Point(x=100, y=22)
    
        """
    
        # Parse and validate the field names.  Validation serves two purposes,
        # generating informative error messages and preventing template injection attacks.
        if isinstance(field_names, basestring):
            field_names = field_names.replace(',', ' ').split() # names separated by whitespace and/or commas
        field_names = tuple(map(str, field_names))
        for name in (typename,) + field_names:
            if not all(c.isalnum() or c=='_' for c in name):
                raise ValueError('Type names and field names can only contain alphanumeric characters and underscores: %r' % name)
            if _iskeyword(name):
                raise ValueError('Type names and field names cannot be a keyword: %r' % name)
            if name[0].isdigit():
                raise ValueError('Type names and field names cannot start with a number: %r' % name)
        seen_names = set()
        for name in field_names:
            if name.startswith('_'):
                raise ValueError('Field names cannot start with an underscore: %r' % name)
            if name in seen_names:
                raise ValueError('Encountered duplicate field name: %r' % name)
            seen_names.add(name)
    
        # Create and fill-in the class template
        numfields = len(field_names)
        argtxt = repr(field_names).replace("'", "")[1:-1]   # tuple repr without parens or quotes
        reprtxt = ', '.join('%s=%%r' % name for name in field_names)
        dicttxt = ', '.join('%r: t[%d]' % (name, pos) for pos, name in enumerate(field_names))
        template = '''class %(typename)s(tuple):
        '%(typename)s(%(argtxt)s)' \n
        __slots__ = () \n
        _fields = %(field_names)r \n
        def __new__(_cls, %(argtxt)s):
            return _tuple.__new__(_cls, (%(argtxt)s)) \n
        @classmethod
        def _make(cls, iterable, new=tuple.__new__, len=len):
            'Make a new %(typename)s object from a sequence or iterable'
            result = new(cls, iterable)
            if len(result) != %(numfields)d:
                raise TypeError('Expected %(numfields)d arguments, got %%d' %% len(result))
            return result \n
        def __repr__(self):
            return '%(typename)s(%(reprtxt)s)' %% self \n
        def _asdict(t):
            'Return a new dict which maps field names to their values'
            return {%(dicttxt)s} \n
        def _replace(_self, **kwds):
            'Return a new %(typename)s object replacing specified fields with new values'
            result = _self._make(map(kwds.pop, %(field_names)r, _self))
            if kwds:
                raise ValueError('Got unexpected field names: %%r' %% kwds.keys())
            return result \n
        def __getnewargs__(self):
            return tuple(self) \n\n''' % locals()
        for i, name in enumerate(field_names):
            template += '        %s = _property(_itemgetter(%d))\n' % (name, i)
        if verbose:
            print template
    
        # Execute the template string in a temporary namespace and
        # support tracing utilities by setting a value for frame.f_globals['__name__']
        namespace = dict(_itemgetter=_itemgetter, __name__='namedtuple_%s' % typename,
                         _property=property, _tuple=tuple)
        try:
            exec template in namespace
        except SyntaxError, e:
            raise SyntaxError(e.message + ':\n' + template)
        result = namespace[typename]
    
        # For pickling to work, the __module__ variable needs to be set to the frame
        # where the named tuple is created.  Bypass this step in enviroments where
        # sys._getframe is not defined (Jython for example).
        if hasattr(_sys, '_getframe'):
            result.__module__ = _sys._getframe(1).f_globals.get('__name__', '__main__')
    
        return result
    
    
class CountingSet(dict):
    """
		Our utility class to implement a set that keeps count of additions/deletions and lets us know when a value is actually created
		or finally removed.
	"""
    
    def inc(self, value):
        """
			Increment the counter for a particular value, return true if the value was created
		"""
        if value in self:
            self[value] += 1
            return False
        else:
            self[value] = 1
            return True
    
    
    def dec(self, value):
        """
			Decrement the counter for a particular value, return true if the value was removed (hit zero).
			Throws key error if not already present.
		"""
        self[value] -= 1
        if self[value] == 0:
            del self[value]
            return True
        return False
    
    
    def printlist(self):
        """
			Print the contents of the current group subscriptions (mainly needed for debugging)
		""" 
        log.debug("Current Groups are: %s ",self.items())
    
    
    def incGroup(self, values):
        """
			Increment the counter for all the values in the incoming list, return the list of values that were created
		"""
        return [v for v in values if self.inc(v)]
    
    
    def decGroup(self, values):
        """
			Decrement the counter for all the values in the incoming list, return the list of values that were removed
		"""
        return [v for v in values if self.dec(v)]

