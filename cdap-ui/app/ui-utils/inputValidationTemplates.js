const NAME = {
    regex: RegExp('^[a-zA-Z][-/\._()a-zA-Z0-9 ]{1,253}[)/a-zA-Z0-9]$'),
    info: 'Name needs to between 3 and 255 characters. May contain spaces, underscore, hyphen, period, forward slash, and parentheses. Can start with alphabets only. Can end with alphanumeric characters, forward slash, and closing parentheses.',
    validate: function(val) {
        return this.regex.test(val) ? true : false;
    }
};

const DEFAULT = {
    regex: RegExp('.*'),
    info: 'Accepts Everything',
    validate: function(val) {
        return this.regex.test(val) ? true : false;
    }
};

const types = {
    'DEFAULT': DEFAULT,
    'NAME': NAME,
};

// getting some jshint errors here!
// export {
//     DEFAULT,
//     NAME,
// };

export default types;
