import React, { FC, useState } from 'react';
import { InterpretationButtons } from "./InterpretationButtons";
import { AddButton } from "../atoms/buttons/AddButton";
import { GenericModal } from "../atoms/GenericModal";
import { PairEditForm } from "./PairEditForm";
import { WhereWhatPair, WorkflowFile } from "maxun-core";
import { AddPair } from "../../api/workflow";
import { Button, Stack } from "@mui/material";
import { FastForward } from "@mui/icons-material";
import { useSocketStore } from "../../context/socket";
import { useGlobalInfoStore } from "../../context/globalInfo";

interface SidePanelHeaderProps {
  setShowOutputData: (show: boolean) => void;
}

export const SidePanelHeader: React.FC<SidePanelHeaderProps> = ({ setShowOutputData }) => {

  const [steppingIsDisabled, setSteppingIsDisabled] = useState(true);
  const { socket } = useSocketStore();

  const handleStep = () => {
    socket?.emit('step');
  };

  return (
    <div style={{ width: 'inherit' }}>
      <InterpretationButtons enableStepping={(isPaused) => setSteppingIsDisabled(!isPaused)} setShowOutputData={setShowOutputData} />
      {/* <Button
       variant='outlined'
       disabled={steppingIsDisabled}
       onClick={handleStep}
       sx={{marginLeft:'15px'}}
     >
       step
       <FastForward/>
     </Button> */}
      <hr />
    </div>
  );
};
