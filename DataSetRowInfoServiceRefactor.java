package everrise.service;
import everrise.daos.MDataSourceDao;
import everrise.daos.DataSetRowInfoListDto;
import everrise.dtos.ColumnDto;
import everrise.dtos.DataSetRowInfoDto;
import everrise.entity.MDataSet;
import everrise.entity.MDataSetRowInfo;
import everrise.daos.MDataSetDao;
import everrise.entity.MDataSource;
import lombok.NonNull;
import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class DataSetRowInfoService extends BaseService {

    private boolean hasWebConnector(MDataSet mDataSet) {
        for (MDataSetRowCreateRule rule : mDataSet.getMDataSetRowCreateRuleList()) {
            if (rule.getTargetDataSource().hasSite()) {
                return true;
            }
        }
        return false;
    }
    private boolean isWebConnectorColumn(ColumnDto columnDto, boolean hasWebConnector) {
        return WEB_CONNECTOR_PHYSICAL_NAME_LIST.contains(columnDto.getPhysicalName()) && hasWebConnector;
    }

    private Long extractDataSourceId(String physicalName) {
        try {
            return Long.valueOf(physicalName.split("_")[0]);
        } catch (NumberFormatException e) {
            throw new RuntimeException("PhysicalName does not match the format.physicalName -> " + physicalName, e);
        }
    }
    private boolean isValidMasterHeaderColumn(MDataSource mDataSource, ColumnDto columnDto, Long dataSourceId) {
        if (mDataSource == null || !mDataSource.hasMasterHeader()) {
            return false;
        }
        String columnName = columnDto.getPhysicalName().replaceFirst(dataSourceId + "_", "");
        return mDataSource.getMMasterHeader().hasColumnPhysicalName(columnName);
    }
    private static final List<String> WEB_CONNECTOR_PHYSICAL_NAME_LIST = Arrays.asList("unknown_id", "customer_id", "customer_identify", "iuid", "first_device_type", "first_os_type", "first_browser_type",
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
            boolean hasWebConnector = hasWebConnector(mDataSet);
            List<DataSetRowInfoDto.ColumnDto> columnDtoList = new ArrayList<>();
            for (ColumnDto columnDto : mDataSetRowInfo.getHeaderMeta().getColumns()) {
                if (isWebConnectorColumn(columnDto,hasWebConnector)){
                    continue;
                }
                Long dataSourceId = extractDataSourceId(columnDto.getPhysicalName().split("_")[0]);
                MDataSource mDataSource = mDataSourceDao.findByIdWithRelation(contractCompanyId, dataSourceId);
                if (isValidMasterHeaderColumn(mDataSource,columnDto,dataSourceId)){
                    columnDtoList.add(new DataSetRowInfoDto.ColumnDto(columnDto));
                    dataSetRowInfoDtoList.add(new DataSetRowInfoDto(mDataSet, columnDtoList));
                }
            }
        }
        DataSetRowInfoListDto dataSetRowInfoListDto = new DataSetRowInfoListDto();
        dataSetRowInfoListDto.setDataSetRowInfoDtoList(dataSetRowInfoDtoList);
        return dataSetRowInfoListDto;
    }
}

