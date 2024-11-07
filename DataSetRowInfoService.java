package everrise.service;
import org.everrise.annountation.Inject;
import org.everrise.annountation.NonNull;
import org.everrise.daos.MDataSetDao;
import org.everrise.daos.MDataSourceDao;
import org.everrise.dtos.ColumnDto;
import org.everrise.dtos.DataSetRowInfoDto;
import org.everrise.entity.MDataSet;
import org.everrise.entity.MDataSetRowCreateRule;
import org.everrise.entity.MDataSetRowInfo;
import org.everrise.entity.MDataSource;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.everrise.service.DataSetRowInfoListDto;

public class DataSetRowInfoService extends BaseService {

  private static List<String> WEB_CONNECTOR_PHYSICAL_NAME_LIST = Arrays.asList("unknown_id",
      "customer_id", "customer_identify", "iuid", "first_device_type", "first_os_type",
      "first_browser_type",
      "first_ip_address", "first_access_datetime", "last_access_datetime", "visit_cnt");

  @Inject
  public MDataSetDao mDataSetDao;
  @Inject
  public MDataSourceDao mDataSourceDao;

  @NonNull
  public DataSetRowInfoListDto getDataSetRowInfoListDto(@NonNull Long contractCompanyId) {
    List<MDataSet> mDataSetList = mDataSetDao.findAllByContractCompanyId(contractCompanyId);
    List<DataSetRowInfoDto> dataSetRowInfoDtoList = new ArrayList<>();

    for (MDataSet mDataSet : mDataSetList) {
      MDataSetRowInfo mDataSetRowInfo = mDataSet.getLatestDataSetRowInfo();

      if (mDataSetRowInfo == null) {
        continue;
      }

      boolean hasWebConnector = false;
      for (MDataSetRowCreateRule rule : mDataSet.getMDataSetRowCreateRuleList()) {
        if (rule.getTargetDataSource().hasSite()) {
          hasWebConnector = true;
          break;
        }
      }

    //   private boolean hasWebConnector(MDataSet mDataSet) {
    //     for (MDataSetRowCreateRule rule : mDataSet.getMDataSetRowCreateRuleList()) {
    //         if (rule.getTargetDataSource().hasSite()) {
    //             return true;
    //         }
    //     }
    //     return false;
    // }
    



      List<DataSetRowInfoDto.ColumnDto> columnDtoList = new ArrayList<>();
      for (ColumnDto columnDto : mDataSetRowInfo.getHeaderMeta().getColumns()) {

        if (WEB_CONNECTOR_PHYSICAL_NAME_LIST.contains(columnDto.getPhysicalName())) {
          if (hasWebConnector) {
            columnDtoList.add(new DataSetRowInfoDto.ColumnDto(columnDto));
          }
          continue;
        }

        // private boolean isWebConnectorColumn(ColumnDto columnDto, boolean hasWebConnector) {
        //     return WEB_CONNECTOR_PHYSICAL_NAME_LIST.contains(columnDto.getPhysicalName()) && hasWebConnector;
        // }
        

        Long dataSourceId = 0L;
        try {
          dataSourceId = Long.valueOf(columnDto.getPhysicalName().split("_")[0]);
        } catch (NumberFormatException e) {
          throw new RuntimeException("physicalName does not match the format. physicalName -> "
              + columnDto.getPhysicalName());
        }

        // private Long extractDataSourceId(String physicalName) {
        //     try {
        //         return Long.valueOf(physicalName.split("_")[0]);
        //     } catch (NumberFormatException e) {
        //         throw new RuntimeException("PhysicalName does not match the format. physicalName -> " + physicalName, e);
        //     }
        // }
        

        MDataSource mDataSource = mDataSourceDao.findByIdWithRelation(contractCompanyId,
            dataSourceId);
        if (mDataSource == null) {
          continue;
        }

        if (!mDataSource.hasMasterHeader()) {
          continue;
        }

        String columnName = columnDto.getPhysicalName().replaceFirst(dataSourceId + "_", "");
        if (mDataSource.getMMasterHeader().hasColumnPhysicalName(columnName)) {
          columnDtoList.add(new DataSetRowInfoDto.ColumnDto(columnDto));
        }

        // private boolean isValidMasterHeaderColumn(MDataSource mDataSource, ColumnDto columnDto, Long dataSourceId) {
        //     if (mDataSource == null || !mDataSource.hasMasterHeader()) {
        //         return false;
        //     }
        //     String columnName = columnDto.getPhysicalName().replaceFirst(dataSourceId + "_", "");
        //     return mDataSource.getMMasterHeader().hasColumnPhysicalName(columnName);
        // }
        
      }

      dataSetRowInfoDtoList.add(new DataSetRowInfoDto(mDataSet, columnDtoList));
    }

    DataSetRowInfoListDto dataSetRowInfoListDto = new DataSetRowInfoListDto();
    dataSetRowInfoListDto.setDataSetRowInfoDtoList(dataSetRowInfoDtoList);
    return dataSetRowInfoListDto;
  }


}

