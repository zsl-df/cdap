import React from 'react';
import PropTypes from 'prop-types';
import {
  FormGroup,
  Input,
  FormFeedback,
  InputGroup,
  InputGroupAddon,
  InputGroupText,
  UncontrolledTooltip,
} from 'reactstrap';
require('./ValidatedInput.scss');

export default function ValidatedInput(props) {
  const {validationError, inputInfo, label, ...moreProps} = props;
  const isInvalid = validationError ? true : false;

  return (
      <FormGroup row style={{paddingLeft:15}}>
        <InputGroup>
          <Input {...moreProps} invalid={isInvalid}/>
          <InputGroupAddon addonType="append" className="input-group-info">
            <InputGroupText id={label.replace(/\s/g,'')}>
              <span className="fa fa-info-circle"></span>
              <UncontrolledTooltip placement="left" target={label.replace(/\s/g,'')}>
                {inputInfo}
              </UncontrolledTooltip>
            </InputGroupText>
          </InputGroupAddon>
        </InputGroup>
        <FormFeedback className="feedback-error">{validationError}</FormFeedback>
      </FormGroup>
  );
}

ValidatedInput.propTypes = {
  type: PropTypes.string,
  size: PropTypes.string,
  label: PropTypes.string,
  required: PropTypes.bool,
  placeholder: PropTypes.string,
  defaultValue: PropTypes.string,
  value: PropTypes.string,
  disabled: PropTypes.bool,
  className: PropTypes.string,
  validationError: PropTypes.string,
  inputInfo: PropTypes.string,
  onChange: PropTypes.func,
  readOnly: PropTypes.bool,
};
