import A1 from '@flighter/a1-notation';
import {
  Column,
  ExecutionOutput,
  IntegrationError,
  GoogleSheetsActionType,
  GoogleSheetsDatasourceConfiguration,
  DatasourceMetadataDto,
  TableType,
  Table,
  GoogleSheetsFormatType,
  GoogleSheetsActionConfiguration,
  GoogleSheetsAuthType,
  validateRowsSchema
} from '@superblocksteam/shared';
import { BasePlugin, PluginExecutionProps } from '@superblocksteam/shared-backend';
import { google, sheets_v4, drive_v3 } from 'googleapis';
import { GoogleAuth, OAuth2Client } from 'googleapis-common';

type CellValueType = boolean | string | number | sheets_v4.Schema$ErrorValue;

const MAX_A1_RANGE = 'ZZZ10000000'; // static limit based on https://support.google.com/drive/answer/37603

class SheetColumn {
  name: string;
  type: string;
  sourceColumnIndex: number;
}

export default class GoogleSheetsPlugin extends BasePlugin {
  async metadata(
    datasourceConfiguration: GoogleSheetsDatasourceConfiguration,
    actionConfiguration?: GoogleSheetsActionConfiguration
  ): Promise<DatasourceMetadataDto> {
    try {
      const [, driveClient, sheetsClient] = this.getGoogleClients(datasourceConfiguration);
      const tables: Table[] = [];
      let nextPageToken: string;
      do {
        const result = await driveClient.files.list({
          q: "mimeType='application/vnd.google-apps.spreadsheet'",
          fields: 'nextPageToken, files(id,name)',
          pageToken: nextPageToken
        });
        nextPageToken = result.data.nextPageToken;
        for (const file of result.data.files) {
          if (actionConfiguration && file.id === actionConfiguration.spreadsheetId) {
            const spreadsheet = await sheetsClient.spreadsheets.get({
              includeGridData: false,
              spreadsheetId: file.id
            });
            const columns: Column[] = [];
            for (const sheet of spreadsheet.data.sheets) {
              columns.push({ name: sheet.properties.title, type: 'column' });
            }
            tables.push({
              id: spreadsheet.data.spreadsheetId,
              type: TableType.TABLE,
              name: spreadsheet.data.properties.title,
              columns: columns
            });
          } else {
            tables.push({
              id: file.id,
              type: TableType.TABLE,
              name: file.name,
              columns: []
            });
          }
        }
      } while (nextPageToken);
      return Promise.resolve({ dbSchema: { tables: tables } });
    } catch (err) {
      throw new IntegrationError(`Failed to get metadata: ${err}`);
    }
  }
  async execute({ datasourceConfiguration, actionConfiguration }: PluginExecutionProps): Promise<ExecutionOutput> {
    try {
      const googleSheetsAction = actionConfiguration.action;
      const ret = new ExecutionOutput();
      switch (googleSheetsAction) {
        case GoogleSheetsActionType.READ_SPREADSHEET:
          ret.output = await this.readFromSpreadsheet(
            datasourceConfiguration as GoogleSheetsDatasourceConfiguration,
            actionConfiguration.spreadsheetId,
            actionConfiguration.sheetTitle,
            actionConfiguration.extractFirstRowHeader,
            actionConfiguration.format
          );
          return ret;
        case GoogleSheetsActionType.READ_SPREADSHEET_RANGE:
          ret.output = await this.readFromSpreadsheet(
            datasourceConfiguration as GoogleSheetsDatasourceConfiguration,
            actionConfiguration.spreadsheetId,
            actionConfiguration.sheetTitle,
            actionConfiguration.extractFirstRowHeader,
            actionConfiguration.format,
            actionConfiguration.range
          );
          return ret;
        case GoogleSheetsActionType.APPEND_SPREADSHEET:
          ret.output = await this.appendToSpreadsheet(
            datasourceConfiguration as GoogleSheetsDatasourceConfiguration,
            actionConfiguration.spreadsheetId,
            actionConfiguration.sheetTitle,
            actionConfiguration.data
          );
          return ret;
      }
      return ret;
    } catch (err) {
      throw new IntegrationError(`Google Sheets request failed. ${err.message}`);
    }
  }
  async appendToSpreadsheet(
    datasourceConfiguration: GoogleSheetsDatasourceConfiguration,
    spreadsheetId: string,
    sheetTitle: string,
    data: string
  ): Promise<ExecutionOutput> {
    const ret = new ExecutionOutput();
    const [, , sheetsClient] = this.getGoogleClients(datasourceConfiguration);
    const [columnNames, rowsNumber] = await this.extractSheetColumns(spreadsheetId, sheetTitle, sheetsClient, true);
    let jsonData;
    try {
      jsonData = JSON.parse(data);
    } catch (err) {
      throw new IntegrationError(`Failed to parse JSON data: ${err.message}`);
    }
    try {
      validateRowsSchema(jsonData);
    } catch (err) {
      throw new IntegrationError(`Validation failed for rows to append: ${err.message}`);
    }
    const rowsData = this.dataToCells(jsonData, columnNames);
    const requestBody: sheets_v4.Schema$ValueRange = {
      range: `${sheetTitle}!A${rowsNumber + 1}:L${rowsNumber + 1}`,
      majorDimension: 'ROWS',
      values: rowsData
    };
    const appendResult = await sheetsClient.spreadsheets.values.append({
      spreadsheetId: spreadsheetId,
      range: `${sheetTitle}!A${rowsNumber + 1}:L${rowsNumber + 1}`,
      requestBody: requestBody,
      valueInputOption: 'RAW'
    });
    if (appendResult.status != 200) {
      throw new IntegrationError(`Failed to append data to Google Sheet, unexpected status: ${appendResult.status}`);
    }
    ret.output = appendResult.data.updates;
    return ret;
  }

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  dataToCells(data: any[], columns: SheetColumn[]): any[][] {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const cells: any[][] = [];
    data.forEach((row) => {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const rowValues: any[] = [];
      Object.keys(row).forEach((key) => {
        if (columns.length > 0) {
          const matchingColumn = columns.find((column) => column.name?.toLowerCase() === key?.toLowerCase());
          if (!matchingColumn) {
            throw new IntegrationError(
              `Unexpected key: "${key}". Expected keys are: ${columns
                .filter((c) => c.name)
                .flatMap((c) => `"${c.name}"`)
                .join(', ')}`
            );
          }
          rowValues[matchingColumn.sourceColumnIndex] = row[key];
        } else {
          rowValues.push(row[key]);
        }
      });
      cells.push(rowValues);
    });
    return cells;
  }

  dynamicProperties(): string[] {
    return ['range', 'data'];
  }

  async test(datasourceConfiguration: GoogleSheetsDatasourceConfiguration): Promise<void> {
    try {
      const [, driveClient] = this.getGoogleClients(datasourceConfiguration);
      const result = await driveClient.files.list({
        q: "mimeType='application/vnd.google-apps.spreadsheet'",
        fields: 'nextPageToken, files(id, name)'
      });
      if (result.status != 200) {
        throw new IntegrationError(`Failed to test Google Sheet, unexpected status: ${result.status}`);
      }
    } catch (err) {
      throw new IntegrationError(`Google Sheets client configuration failed. ${err.message}`);
    }
  }

  async preDelete(datasourceConfiguration: GoogleSheetsDatasourceConfiguration): Promise<void> {
    try {
      if (datasourceConfiguration.authType === GoogleSheetsAuthType.SERVICE_ACCOUNT || !datasourceConfiguration.authConfig.authToken) {
        // if there is no auth token - nothing to revoke
        return;
      }
      const [authClient, ,] = this.getGoogleClients(datasourceConfiguration);
      const revokationResult = await (authClient as OAuth2Client).revokeCredentials();
      if (revokationResult.status != 200) {
        throw new IntegrationError(
          `Failed to revoke token, unexpected HTTP status: ${revokationResult.status}, response: ${revokationResult.data}`
        );
      }
    } catch (err) {
      const httpCode: string = err.status ?? err.code;
      switch (httpCode) {
        case '400': {
          console.log(`Failed to revoke a token: ${err.message}`);
          break;
        }
        default: {
          throw err;
        }
      }
    }
  }

  async readFromSpreadsheet(
    datasourceConfiguration: GoogleSheetsDatasourceConfiguration,
    spreadsheetId?: string | undefined,
    sheetTitle?: string | undefined,
    extractFirstRowHeader?: boolean,
    format = GoogleSheetsFormatType.FORMATTED_VALUE,
    range?: string
  ): Promise<CellValueType[]> {
    if (range && !A1.isValid(range)) {
      throw new IntegrationError(`The provided range ${range} is invalid`);
    }
    const [, , sheetsClient] = this.getGoogleClients(datasourceConfiguration);
    const params: sheets_v4.Params$Resource$Spreadsheets$Get = {
      includeGridData: true,
      spreadsheetId: spreadsheetId
    };
    let columnNamesOffset = 0;
    const [columnNames] = await this.extractSheetColumns(spreadsheetId, sheetTitle, sheetsClient, extractFirstRowHeader);
    if (range && extractFirstRowHeader) {
      const a1Range = new A1(range);
      if (range != a1Range.toString()) {
        throw new IntegrationError(`The provided range ${range} is invalid`);
      }
      // return empty set if user had specified A1:XXX and row 1 is used as Table header
      if (a1Range.getHeight() === 1 && a1Range.getRow() === 1) {
        return this.sheetDataToRecordSet([], format, columnNames, columnNamesOffset);
      }
      // skip 1st row if it's used as a header
      const adjustedRange = a1Range.getRow() === 1 ? a1Range.removeY(-1) : a1Range;
      params.ranges = [`${sheetTitle}!${adjustedRange}`];
      columnNamesOffset = a1Range.getCol() - 1;
    } else if (range) {
      params.ranges = [`${sheetTitle}!${range}`];
    } else if (extractFirstRowHeader) {
      params.ranges = [`${sheetTitle}!A2:${MAX_A1_RANGE}`];
    } else {
      params.ranges = [`${sheetTitle}!A1:${MAX_A1_RANGE}`];
    }
    const result = await sheetsClient.spreadsheets.get(params);
    return this.sheetDataToRecordSet(result.data.sheets, format, columnNames, columnNamesOffset);
  }

  sheetDataToRecordSet(
    sheetData: sheets_v4.Schema$Sheet[],
    format: GoogleSheetsFormatType,
    sheetColumns: SheetColumn[],
    columnNamesOffset: number
  ): Record<string, CellValueType>[] {
    const recordsSet: Record<string, CellValueType>[] = [];
    let columnIndex = 0;
    sheetData?.forEach((sheetDataItem) => {
      sheetDataItem?.data?.forEach((sheetDataItemDataItem) => {
        sheetDataItemDataItem?.rowData?.forEach((row) => {
          const currentRow: Record<string, CellValueType> = {};
          columnIndex = 0;
          row?.values?.forEach((cellData) => {
            let columnName: string;
            if (sheetColumns[columnIndex + columnNamesOffset]) {
              columnName = sheetColumns[columnIndex + columnNamesOffset].name;
            } else {
              columnName = this.toExcelColumnName(columnIndex + columnNamesOffset);
            }
            currentRow[columnName] = this.extractCellValue(cellData, format);
            columnIndex++;
          });
          recordsSet.push(currentRow);
        });
      });
    });
    return recordsSet;
  }

  extractCellValue(cellData: sheets_v4.Schema$CellData, format: GoogleSheetsFormatType): CellValueType {
    if (format === GoogleSheetsFormatType.EFFECTIVE_VALUE) {
      return this.extractExtendedValue(cellData.effectiveValue);
    } else if (format === GoogleSheetsFormatType.USER_ENTERED_VALUE) {
      return this.extractExtendedValue(cellData.userEnteredValue);
    } else if (format === GoogleSheetsFormatType.FORMATTED_VALUE) {
      return cellData.formattedValue ?? '';
    }
  }

  extractExtendedValue(extendedValue: sheets_v4.Schema$ExtendedValue): CellValueType {
    return (
      extendedValue.stringValue ??
      extendedValue.numberValue ??
      extendedValue.boolValue ??
      extendedValue.errorValue ??
      extendedValue.formulaValue
    );
  }

  getGoogleClients(
    datasourceConfiguration: GoogleSheetsDatasourceConfiguration
  ): [OAuth2Client | GoogleAuth, drive_v3.Drive, sheets_v4.Sheets] {
    let authClient;
    if (datasourceConfiguration.authType === GoogleSheetsAuthType.OAUTH2_CODE && !datasourceConfiguration.authConfig.authToken) {
      throw new IntegrationError(`Authentication has failed. Please ensure you're connected to your Google account.`);
    } else if (datasourceConfiguration.authType === GoogleSheetsAuthType.SERVICE_ACCOUNT) {
      // TODO(taha) [defer] - Both here and in the bigquery plugin, add validation for the service account
      // credentials object, and log a more descriptive error message
      try {
        const credentials = JSON.parse(datasourceConfiguration.authConfig.googleServiceAccount.value ?? '');
        authClient = new google.auth.GoogleAuth({
          credentials,
          scopes: datasourceConfiguration.authConfig.scope
        });
      } catch (err) {
        throw new IntegrationError(`Failed to parse the service account object. Error:\n${err}`);
      }
    } else {
      authClient = new google.auth.OAuth2({});
      authClient.setCredentials({
        access_token: datasourceConfiguration.authConfig.authToken
      });
    }
    google.options({ auth: authClient });
    const driveClient = google.drive('v3');
    const sheetsClient = google.sheets('v4');
    return [authClient, driveClient, sheetsClient];
  }

  async extractSheetColumns(
    spreadsheetId: string,
    sheetTitle: string,
    sheetsClient: sheets_v4.Sheets,
    extractFirstRowHeader: boolean
  ): Promise<[SheetColumn[], number]> {
    const columns: SheetColumn[] = [];
    let rowsNumber = 0;
    if (extractFirstRowHeader) {
      const result = await sheetsClient.spreadsheets.values.get({
        spreadsheetId: spreadsheetId,
        range: `${sheetTitle}!A1:${MAX_A1_RANGE}`
      });
      let columnIndex = 0;
      result.data?.values.forEach((row) => {
        if (rowsNumber === 0) {
          row?.forEach((cellData) => {
            columns.push({
              name: cellData,
              type: 'sheet',
              sourceColumnIndex: columnIndex++
            });
          });
        }
        rowsNumber++;
      });
    }
    return [columns, rowsNumber];
  }

  toExcelColumnName(columnIndex: number): string {
    //TODO: convert column index to Excel column name(e.g. 3->C)
    return `column${columnIndex}`;
  }
}
