import DOMPurify from 'dompurify';

const TEMPLATES = {
    'simple': {
        ALLOWED_TAGS: [],
    },
};

const dom_sanitizer = DOMPurify.sanitize;

const inputSanitizer = ({dirty = '', inputName = '', config = 'simple'} = {}) => {

    if (config === 'uri') {
        return {
            'clean': encodeURIComponent(dirty),
            'error': null
        };
    }

    const clean = dom_sanitizer(dirty, TEMPLATES[config]);
    // TODO: need a better error code!
    const error = 'Invalid Input' + (inputName ? ': '  + inputName : inputName);
    return {
        'clean': clean,
        'error': clean !== dirty ? error : null,
    };
};

export default inputSanitizer;
