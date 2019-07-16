import React from 'react';
import PropTypes from 'prop-types';
import {
  FormGroup,
  Label,
  Input,
  FormText,
  InputGroup,
  InputGroupAddon,
  InputGroupText,
  Col
} from 'reactstrap';

export default function ValidatedInput(props) {
  const {validationError, inputInfo, label, required, ...moreProps} = props;
  const invalidProp = validationError ? true : false;

  return (
      <FormGroup row>
        <Label for={label} sm={2}>
          {label}
          {
            required ?
            <span> * </span> :
            null
          }
        </Label>
        <Col sm={10}>
        <InputGroup>
          <Input {...moreProps} invalid={invalidProp} id={label}/>
          <InputGroupAddon addonType="append">
            <InputGroupText>
              <span title={inputInfo}>info</span>
            </InputGroupText>
          </InputGroupAddon>
        </InputGroup>
        <FormText>{validationError}</FormText>
        </Col>
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
  className: PropTypes.string,
  validationError: PropTypes.string,
  inputInfo: PropTypes.string,
  onChange: PropTypes.func,
};
