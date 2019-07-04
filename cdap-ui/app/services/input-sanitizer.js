class InputSanitizer {
    constructor() {
        this.TEMPLATES = {
            'simple': {
                ALLOWED_TAGS: [],
            },
        };
        this.dom_sanitizer = ['window'].DOMPurify.sanitize;
        // this.dom_sanitizer = DOMPurify.sanitize;
        // this.dom_sanitizer = dompurify.sanitize;
    }

    sanitize(dirty, config = 'simple') {
        const clean = this.dom_sanitizer(dirty, this.TEMPLATES[config]);
        // TODO: need a better error code!
        const error = 'Error occured';
        return {
            'clean': clean,
            'error': error,
        };
    }
}

angular.module(PKG.name+'.services').factory('inputSanitizer', InputSanitizer);
