/*
 * Copyright © 2018 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

import * as React from 'react';
import { Modal, ModalHeader, ModalBody, ModalFooter } from 'reactstrap';
import {getCurrentNamespace} from 'services/NamespaceStore';
import T from 'i18n-react';
import LoadingSVG from 'components/LoadingSVG';
import MyDataPrepApi from 'api/dataprep';
import CardActionFeedback, {CARD_ACTION_TYPES} from 'components/CardActionFeedback';
import {objectQuery} from 'services/helpers';
import BtnWithLoading from 'components/BtnWithLoading';
import {ConnectionType} from 'components/DataPrepConnections/ConnectionType';

const PREFIX = 'features.DataPrepConnections.AddConnections.ADLS';
const ADDCONN_PREFIX = 'features.DataPrepConnections.AddConnections';

const LABEL_COL_CLASS = 'col-xs-3 col-form-label text-xs-right';
const INPUT_COL_CLASS = 'col-xs-8';

require('./ADLSConnection.scss');

enum ConnectionMode {
  Add = 'ADD',
  Edit = 'EDIT',
  Duplicate = 'DUPLICATE',
}

interface IADLSConnectionProps {
  close: () => void;
  onAdd: () => void;
  mode: ConnectionMode;
  connectionId: string;
}

interface IADLSConnectionState {
  error?: string | object | null;
  name?: string;
  KVUrl?: string;
  JcekPath?: string;
  ADLS_Directory?: string;
  clientID?: string;
  clientKey?: string;
  authTokenURL?: string;
  testConnectionLoading?: boolean;
  connectionResult?: {
    message?: string;
    type?: string
  };
  loading?: boolean;
  isKVorJCEK?: boolean;
}

interface IProperties {
  KVUrl?: string;
  JcekPath?: string;
  ADLS_Directory?: string;
  clientID?: string;
  clientKey?: string;
  authTokenURL?: string;
  testConnectionLoading?: boolean;
}

export default class ADLSConnection extends React.PureComponent<IADLSConnectionProps, IADLSConnectionState> {
  public state: IADLSConnectionState = {
    error: null,
    name: '',
    KVUrl: '',
    JcekPath: '',
    clientID: '',
    clientKey: '',
    authTokenURL: '',
    ADLS_Directory: '',
    testConnectionLoading: false,
    connectionResult: {
      message: '',
      type: '',
    },
    isKVorJCEK: true,
    loading: false,
  };

  public componentDidMount() {
    if (this.props.mode === ConnectionMode.Add) {
      return;
    }

    this.setState({loading: true, isKVorJCEK: true});

    const namespace = getCurrentNamespace();

    const params = {
      namespace,
      connectionId: this.props.connectionId,
    };

    MyDataPrepApi.getConnection(params)
      .subscribe((res) => {
        const info = objectQuery(res, 'values', 0);
        const name = this.props.mode === ConnectionMode.Edit ? info.name : '';
        const JcekPath = objectQuery(info, 'properties', 'JcekPath');
        const ADLS_Directory = objectQuery(info, 'properties', 'ADLS_Directory');
        const KVUrl = objectQuery(info, 'properties', 'KVUrl');
        const clientID = objectQuery(info, 'properties', 'clientID');
        const clientKey = objectQuery(info, 'properties', 'clientKey');
        const authTokenURL = objectQuery(info, 'properties', 'authTokenURL');

        this.setState({
          name,
          KVUrl,
          JcekPath,
          ADLS_Directory,
          clientID,
          clientKey,
          authTokenURL,
          loading: false,
        });
      }, (err) => {
        const error = objectQuery(err, 'response', 'message') || objectQuery(err, 'response') || err;

        this.setState({
          loading: false,
          error
        });
      });
  }

  private constructProperties = (): IProperties => {
    const properties: IProperties = {};

    if (this.state.isKVorJCEK) {
      if (this.state.KVUrl && this.state.KVUrl.length > 0) {
        properties.KVUrl = this.state.KVUrl;
      }
      if (this.state.JcekPath && this.state.JcekPath.length > 0) {
        properties.JcekPath = this.state.JcekPath;
      }
    } else {
      if (this.state.clientID && this.state.clientID.length > 0) {
        properties.clientID = this.state.clientID;
      }
      if (this.state.clientKey && this.state.clientKey.length > 0) {
        properties.clientKey = this.state.clientKey;
      }
      if (this.state.authTokenURL && this.state.authTokenURL.length > 0) {
        properties.authTokenURL = this.state.authTokenURL;
      }
    }

    if (this.state.ADLS_Directory && this.state.ADLS_Directory.length > 0) {
      properties.ADLS_Directory = this.state.ADLS_Directory;
    }

    return properties;
  }

  private addConnection = () => {
    const namespace = getCurrentNamespace();

    const requestBody = {
      name: this.state.name,
      type: ConnectionType.ADLS,
      properties: this.constructProperties(),
    };

    MyDataPrepApi.createConnection({namespace}, requestBody)
      .subscribe(() => {
        this.setState({error: null});
        this.props.onAdd();
        this.props.close();
      }, (err) => {
        const error = objectQuery(err, 'response', 'message') || objectQuery(err, 'response') || err;
        this.setState({ error });
      });
  }

  private editConnection = () => {
    const namespace = getCurrentNamespace();

    const params = {
      namespace,
      connectionId: this.props.connectionId,
    };

    const requestBody = {
      name: this.state.name,
      id: this.props.connectionId,
      type: ConnectionType.ADLS,
      properties: this.constructProperties(),
    };

    MyDataPrepApi.updateConnection(params, requestBody)
      .subscribe(() => {
        this.setState({error: null});
        this.props.onAdd();
        this.props.close();
      }, (err) => {
        const error = objectQuery(err, 'response', 'message') || objectQuery(err, 'response') || err;
        this.setState({ error });
      });
  }

  private testConnection = () => {
    this.setState({
      testConnectionLoading: true,
      connectionResult: {
        message: '',
        type: '',
      },
      error: null,
    });

    const namespace = getCurrentNamespace();

    const requestBody = {
      name: this.state.name,
      type: ConnectionType.ADLS,
      properties: this.constructProperties(),
    };

    MyDataPrepApi.adlsTestConnection({namespace}, requestBody)
      .subscribe((res) => {
        this.setState({
          connectionResult: {
            type: CARD_ACTION_TYPES.SUCCESS,
            message: res.message,
          },
          testConnectionLoading: false,
        });
      }, (err) => {
        const errorMessage = objectQuery(err, 'response', 'message') ||
        objectQuery(err, 'response') ||
        T.translate(`${PREFIX}.defaultTestErrorMessage`);

        this.setState({
          connectionResult: {
            type: CARD_ACTION_TYPES.DANGER,
            message: errorMessage,
          },
          testConnectionLoading: false,
        });
      });
  }

  private handleChange = (key: string, e: React.ChangeEvent<HTMLInputElement>) => {
    this.setState({
      [key]: e.target.value,
    });
  }

  private renderTestButton = () => {
    const disabled = !this.state.name;

    return (
      <span className="test-connection-button">
        <BtnWithLoading
          className="btn btn-secondary"
          onClick={this.testConnection}
          disabled={disabled}
          loading={this.state.testConnectionLoading}
          label={T.translate(`${PREFIX}.testConnection`)}
          darker={true}
        />
      </span>
    );
  }

  private renderAddConnectionButton = () => {
    let check;
    if (this.state.isKVorJCEK) {
      check = this.state.KVUrl && this.state.JcekPath;
    } else {
      check = this.state.clientID && this.state.clientKey && this.state.authTokenURL;
    }
    const disabled = !(this.state.name && check && this.state.ADLS_Directory) || this.state.testConnectionLoading;

    let onClickFn = this.addConnection;

    if (this.props.mode === ConnectionMode.Edit) {
      onClickFn = this.editConnection;
    }

    return (
      <ModalFooter>
        <button
          className="btn btn-primary"
          onClick={onClickFn}
          disabled={disabled}
        >
          {T.translate(`${PREFIX}.Buttons.${this.props.mode}`)}
        </button>

        {this.renderTestButton()}
      </ModalFooter>
    );
  }

  private handleOptionChange = (currentTarget) => {
    this.setState({isKVorJCEK : currentTarget.target.value === 'kvURL_jcekpath'});
  }

  private renderContent() {
    if (this.state.loading) {
      return (
        <div className="adls-detail text-xs-center">
          <br />
          <LoadingSVG />
        </div>
      );
    }
    let renderOption;

    if (this.state.isKVorJCEK) {
      renderOption = (<div className='kvurl-jcekpath'>
        <div className="form-group row">
          <label className={LABEL_COL_CLASS}>
            {T.translate(`${PREFIX}.KVUrl`)}
            <span className="asterisk">*</span>
          </label>
          <div className={INPUT_COL_CLASS}>
            <div className="input-text">
              <input
                type="text"
                className="form-control"
                value={this.state.KVUrl}
                onChange={this.handleChange.bind(this, 'KVUrl')}
                placeholder={T.translate(`${PREFIX}.Placeholders.KVUrl`).toString()}
              />
            </div>
          </div>
        </div>

        <div className="form-group row">
          <label className={LABEL_COL_CLASS}>
            {T.translate(`${PREFIX}.JcekPath`)}
            <span className="asterisk">*</span>
          </label>
          <div className={INPUT_COL_CLASS}>
            <div className="input-text">
              <input
                type="text"
                className="form-control"
                value={this.state.JcekPath}
                onChange={this.handleChange.bind(this, 'JcekPath')}
                placeholder={T.translate(`${PREFIX}.Placeholders.JcekPath`).toString()}
              />
            </div>
          </div>
        </div>
      </div>);
    } else {
      renderOption = (<div className='clientID'>
        <div className="form-group row">
          <label className={LABEL_COL_CLASS}>
            {T.translate(`${PREFIX}.clientID`)}
            <span className="asterisk">*</span>
          </label>
          <div className={INPUT_COL_CLASS}>
            <div className="input-text">
              <input
                type="text"
                className="form-control"
                value={this.state.clientID}
                onChange={this.handleChange.bind(this, 'clientID')}
                placeholder={T.translate(`${PREFIX}.Placeholders.clientID`).toString()}
              />
            </div>
          </div>
        </div>

        <div className="form-group row">
          <label className={LABEL_COL_CLASS}>
            {T.translate(`${PREFIX}.clientKey`)}
            <span className="asterisk">*</span>
          </label>
          <div className={INPUT_COL_CLASS}>
            <div className="input-text">
              <input
                type="text"
                className="form-control"
                value={this.state.clientKey}
                onChange={this.handleChange.bind(this, 'clientKey')}
                placeholder={T.translate(`${PREFIX}.Placeholders.clientKey`).toString()}
              />
            </div>
          </div>
        </div>

        <div className="form-group row">
          <label className={LABEL_COL_CLASS}>
            {T.translate(`${PREFIX}.authTokenURL`)}
            <span className="asterisk">*</span>
          </label>
          <div className={INPUT_COL_CLASS}>
            <div className="input-text">
              <input
                type="text"
                className="form-control"
                value={this.state.authTokenURL}
                onChange={this.handleChange.bind(this, 'authTokenURL')}
                placeholder={T.translate(`${PREFIX}.Placeholders.authTokenURL`).toString()}
              />
            </div>
          </div>
        </div>
      </div>);
    }

    return (
      <div className="adls-detail">
        <div className="form">
          <div className="form-group row">
            <label className={LABEL_COL_CLASS}>
              {T.translate(`${PREFIX}.name`)}
              <span className="asterisk">*</span>
            </label>
            <div className={INPUT_COL_CLASS}>
              <div className="input-text">
                <input
                  type="text"
                  className="form-control"
                  value={this.state.name}
                  onChange={this.handleChange.bind(this, 'name')}
                  disabled={this.props.mode === ConnectionMode.Edit}
                  placeholder={T.translate(`${PREFIX}.Placeholders.name`).toString()}
                />
              </div>
            </div>
          </div>

          <div className="form-group row">
            <label className={LABEL_COL_CLASS}>
              {T.translate(`${PREFIX}.ADLS_Directory`)}
              <span className="asterisk">*</span>
            </label>
            <div className={INPUT_COL_CLASS}>
              <div className="input-text">
                <input
                  type="text"
                  className="form-control"
                  value={this.state.ADLS_Directory}
                  onChange={this.handleChange.bind(this, 'ADLS_Directory')}
                  placeholder={T.translate(`${PREFIX}.Placeholders.ADLS_Directory`).toString()}
                />
              </div>
            </div>
          </div>

          <div className="form-group row">
            <label className={LABEL_COL_CLASS}></label>
            <div>
              <input
                type="radio"
                value="kvURL_jcekpath"
                name="option"
                checked={this.state.isKVorJCEK}
                onChange={this.handleOptionChange.bind(this)}/> kvURL_jcekpath
              <input
                type="radio"
                value="clientID"
                name="option"
                checked={!this.state.isKVorJCEK}
                onChange={this.handleOptionChange.bind(this)}/> clientID
            </div>
          </div>
          {renderOption}
        </div>
      </div>
    );
  }

  private renderMessage() {
    const connectionResult = this.state.connectionResult;

    if (!this.state.error && !connectionResult.message) { return null; }

    if (this.state.error) {
      return (
        <CardActionFeedback
          type={connectionResult.type}
          message={T.translate(`${PREFIX}.ErrorMessages.${this.props.mode}`)}
          extendedMessage={this.state.error}
        />
      );
    }

    const connectionResultType = connectionResult.type;
    const extendedMessage = connectionResultType === CARD_ACTION_TYPES.SUCCESS ? null : connectionResult.message;

    return (
      <CardActionFeedback
        message={T.translate(`${ADDCONN_PREFIX}.TestConnectionLabels.${connectionResultType.toLowerCase()}`)}
        extendedMessage={extendedMessage}
        type={connectionResultType}
      />
    );
  }

  private renderModalFooter = () => {
    return this.renderAddConnectionButton();
  }

  public render() {
    return (
      <div>
        <Modal
          isOpen={true}
          toggle={this.props.close}
          size="lg"
          className="adls-connection-modal cdap-modal"
          backdrop="static"
          zIndex="1061"
        >
          <ModalHeader toggle={this.props.close}>
            {T.translate(`${PREFIX}.ModalHeader.${this.props.mode}`, {connection: this.props.connectionId})}
          </ModalHeader>

          <ModalBody>
            {this.renderContent()}
          </ModalBody>

          {this.renderModalFooter()}
          {this.renderMessage()}
        </Modal>
      </div>
    );
  }
}
